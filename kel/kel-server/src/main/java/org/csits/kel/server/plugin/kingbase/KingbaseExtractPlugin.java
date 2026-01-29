package org.csits.kel.server.plugin.kingbase;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Component;

/**
 * 基于 PostgreSQL 驱动实现的 Kingbase 卸载插件。
 *
 * 当前实现：
 * - 仅支持 JOB 类型为 EXTRACT_KINGBASE 的作业；
 * - 对配置中的每个 ExtractTask / table，执行 SELECT * 并写出到 TXT 文件；
 * - 分隔符/命名规则后续可按规范进一步完善。
 */
@Slf4j
@Component
public class KingbaseExtractPlugin implements ExtractPlugin {

    @Override
    public boolean supports(Object context) {
        if (!(context instanceof TaskExecutionContext)) {
            return false;
        }
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig.JobBasic job = ctx.getJobConfig().getJob();
        return job != null && job.getType() == JobType.EXTRACT_KINGBASE;
    }

    @Override
    public void extract(Object context) throws Exception {
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig config = ctx.getJobConfig();
        JobConfig.ExtractDatabaseConfig db = config.getExtractDatabase();
        if (db == null) {
            log.warn("作业 {} 未配置 extract_database，跳过数据库卸载", ctx.getJobCode());
            return;
        }
        String url = String.format("jdbc:postgresql://%s:%d/%s",
            db.getHost(), db.getPort(), db.getName());
        log.info("连接数据库 url={}, user={}", url, db.getUser());
        try (Connection conn = DriverManager.getConnection(url, db.getUser(), db.getPassword())) {
            if (config.getExtractTasks() == null || config.getExtractTasks().isEmpty()) {
                // 没有配置任务时，导出 demo_source 作为示例
                exportTable(conn, "demo_source", ctx);
            } else {
                for (JobConfig.ExtractTaskConfig task : config.getExtractTasks()) {
                    // 全量表导出
                    if (task.getTables() != null) {
                        for (String table : task.getTables()) {
                            exportTable(conn, table, ctx);
                        }
                    }
                    // 增量 SQL 导出
                    if (task.getSqlList() != null) {
                        for (JobConfig.SqlItem sqlItem : task.getSqlList()) {
                            exportSql(conn, sqlItem, ctx);
                        }
                    }
                }
            }
        }
    }

    private void exportTable(Connection conn, String table, TaskExecutionContext context)
        throws Exception {
        JobConfig jobConfig = context.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && context.getGlobalConfig().getExtract() != null) {
            baseWorkDir = context.getGlobalConfig().getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        Path dir = Path.of(baseWorkDir, jobName, context.getBatchNumber(), "data");
        Files.createDirectories(dir);
        String fileName = table.replace('.', '_') + ".txt";
        Path file = dir.resolve(fileName);

        String sql = "SELECT * FROM " + table;
        log.info("导出表 {} 到 {}", table, file);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql);
             Writer writer = new OutputStreamWriter(
                 new FileOutputStream(file.toFile()), StandardCharsets.UTF_8)) {

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        line.append('\u001E'); // 简单用 0x1E 分隔
                    }
                    Object val = rs.getObject(i);
                    if (val != null) {
                        line.append(val.toString());
                    }
                }
                line.append('\n');
                writer.write(line.toString());
            }
        }
    }

    private void exportSql(Connection conn, JobConfig.SqlItem sqlItem, TaskExecutionContext context)
        throws Exception {
        JobConfig jobConfig = context.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && context.getGlobalConfig().getExtract() != null) {
            baseWorkDir = context.getGlobalConfig().getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        Path dir = Path.of(baseWorkDir, jobName, context.getBatchNumber(), "data");
        Files.createDirectories(dir);
        String fileName = sqlItem.getName() + ".txt";
        Path file = dir.resolve(fileName);

        log.info("按 SQL [{}] 导出到 {}", sqlItem.getName(), file);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sqlItem.getSql());
             Writer writer = new OutputStreamWriter(
                 new FileOutputStream(file.toFile()), StandardCharsets.UTF_8)) {

            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        line.append('\u001E');
                    }
                    Object val = rs.getObject(i);
                    if (val != null) {
                        line.append(val.toString());
                    }
                }
                line.append('\n');
                writer.write(line.toString());
            }
        }
    }
}

