package org.csits.kel.server.plugin.kingbase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.plugin.LoadPlugin;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.constants.LoadMode;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Component;

/**
 * 人大金仓结构化数据加载插件。解包后按 TRUNCATE_LOAD/APPEND/MERGE 策略执行 COPY FROM 到目标库。
 */
@Slf4j
@Component
public class KingbaseLoadPlugin implements LoadPlugin {

    @Override
    public boolean supports(Object context) {
        if (!(context instanceof TaskExecutionContext)) {
            return false;
        }
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig.JobBasic job = ctx.getJobConfig().getJob();
        return job != null && job.getType() == JobType.KINGBASE_LOAD;
    }

    @Override
    public void load(Object context) throws Exception {
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig config = ctx.getJobConfig();
        JobConfig.TargetDatabaseConfig target = config.getTargetDatabase();
        if (target == null) {
            log.warn("作业 {} 未配置 target_database，跳过数据库加载", ctx.getJobName());
            return;
        }
        Path dataDir = resolveWorkDir(ctx).resolve("data");
        if (Files.notExists(dataDir) || !Files.isDirectory(dataDir)) {
            log.warn("作业 {} 工作目录下无 data 目录: {}", ctx.getJobName(), dataDir);
            return;
        }
        String url = String.format("jdbc:postgresql://%s:%d/%s",
            target.getHost(), target.getPort(), target.getName());
        log.info("加载目标库 url={}, user={}", url, target.getUser());
        Map<String, Long> loadTableStats = new LinkedHashMap<>();
        try (Connection conn = DriverManager.getConnection(url, target.getUser(), target.getPassword())) {
            List<JobConfig.LoadTaskConfig> loadTasks = config.getLoadTasks();
                if (loadTasks == null || loadTasks.isEmpty()) {
                    log.info("作业 {} 未配置 load_tasks，跳过加载", ctx.getJobName());
                ctx.setAttribute("loadTableStats", loadTableStats);
                return;
            }
            for (JobConfig.LoadTaskConfig task : loadTasks) {
                LoadMode mode = task.getType() != null ? task.getType() : LoadMode.APPEND;
                Map<String, String> mapping = task.getInterfaceMapping();
                if (mapping == null || mapping.isEmpty()) {
                    continue;
                }
                boolean useTransaction = Boolean.TRUE.equals(task.getEnableTransaction());
                if (useTransaction) {
                    conn.setAutoCommit(false);
                }
                try {
                    for (Map.Entry<String, String> e : mapping.entrySet()) {
                        String sourceKey = e.getKey();
                        String targetTable = e.getValue();
                        Path file = resolveDataFile(dataDir, sourceKey);
                        if (file == null) {
                            log.warn("未找到对应数据文件: {} -> {}", sourceKey, targetTable);
                            continue;
                        }
                        if (mode == LoadMode.TRUNCATE_LOAD) {
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute("TRUNCATE TABLE " + targetTable);
                            }
                        }
                        long rows = copyFromFile(conn, file, targetTable);
                        loadTableStats.merge(targetTable, rows, Long::sum);
                        log.info("已加载 {} -> {}，写入 {} 行", file.getFileName(), targetTable, rows);
                    }
                    if (task.getSqlList() != null && !task.getSqlList().isEmpty()) {
                        for (JobConfig.SqlItem sql : task.getSqlList()) {
                            if (sql.getSql() != null && !sql.getSql().trim().isEmpty()) {
                                try (Statement stmt = conn.createStatement()) {
                                    stmt.execute(sql.getSql());
                                }
                            }
                        }
                    }
                    if (useTransaction) {
                        conn.commit();
                    }
                } catch (Exception ex) {
                    if (useTransaction) {
                        conn.rollback();
                    }
                    throw ex;
                } finally {
                    if (useTransaction) {
                        conn.setAutoCommit(true);
                    }
                }
            }
            ctx.setAttribute("loadTableStats", loadTableStats);
        }
    }

    private Path resolveWorkDir(TaskExecutionContext ctx) {
        JobConfig jobConfig = ctx.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && ctx.getGlobalConfig().getExtract() != null) {
            baseWorkDir = ctx.getGlobalConfig().getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        return Paths.get(baseWorkDir, jobName, ctx.getBatchNumber());
    }

    private Path resolveDataFile(Path dataDir, final String sourceKey) throws IOException {
        String base = sourceKey;
        if (base.endsWith(".txt")) {
            base = base.substring(0, base.length() - 4);
        }
        final String baseName = base;
        Path p = dataDir.resolve(baseName + ".txt");
        if (Files.isRegularFile(p)) {
            return p;
        }
        Path withUnderscore = dataDir.resolve(baseName.replace('.', '_') + ".txt");
        if (Files.isRegularFile(withUnderscore)) {
            return withUnderscore;
        }
        try (java.util.stream.Stream<Path> stream = Files.list(dataDir)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(f -> {
                    String name = f.getFileName().toString();
                    return name.equals(sourceKey) || name.equals(baseName + ".txt")
                        || name.startsWith(baseName + "_") || name.startsWith(baseName + ".");
                })
                .findFirst()
                .orElse(null);
        }
    }

    /**
     * 使用 COPY FROM STDIN 由客户端读文件并写入服务端，无需 pg_read_server_files 权限。
     * @return 写入的行数
     */
    private long copyFromFile(Connection conn, Path file, String targetTable) throws Exception {
        String copySql = "COPY " + targetTable + " FROM STDIN WITH (FORMAT text, DELIMITER E'\\x1E', ENCODING 'UTF-8', NULL '')";
        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        try (Reader reader = new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8)) {
            return copyManager.copyIn(copySql, reader);
        }
    }
}
