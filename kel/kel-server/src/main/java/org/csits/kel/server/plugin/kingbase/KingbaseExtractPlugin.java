package org.csits.kel.server.plugin.kingbase;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.FileNamingService;
import org.csits.kel.server.service.MetricsCollector;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.stereotype.Component;

/**
 * 基于 PostgreSQL 驱动实现的 Kingbase 卸载插件。
 *
 * 改进实现：
 * - 使用 COPY TO STDOUT 命令高效导出数据（无需服务器文件权限）
 * - 自动fallback到COPY TO文件路径（需要服务器权限）
 * - 返回导出元数据供manifest使用
 * - 支持标准文件命名规范
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KingbaseExtractPlugin implements ExtractPlugin {

    private final FileNamingService fileNamingService;
    private final MetricsCollector metricsCollector;
    private final AtomicInteger sequenceGenerator = new AtomicInteger(1);

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
            log.warn("作业 {} 未        配置 extract_database，跳过数据库卸载", ctx.getJobName());
            return;
        }
        String url = String.format("jdbc:postgresql://%s:%d/%s",
            db.getHost(), db.getPort(), db.getName());
        log.info("连接数据库 url={}, user={}", url, db.getUser());

        // 获取并发度配置
        int concurrency = getConcurrency(ctx);
        log.info("使用并发度: {}", concurrency);

        // 收集所有需要导出的表和SQL
        List<ExportTask> exportTasks = collectExportTasks(config, ctx);

        List<TableExportResult> results;
        if (concurrency <= 1 || exportTasks.size() <= 1) {
            // 串行导出（单线程或单任务）
            results = exportSerially(url, db, exportTasks);
        } else {
            // 并发导出（多线程）
            results = exportConcurrently(url, db, exportTasks, concurrency);
        }

        // 将导出结果存储到context中供manifest使用
        ctx.setAttribute("exportResults", results);

        // 记录表统计信息到MetricsCollector
        Long taskId = ctx.getTaskId();
        for (TableExportResult result : results) {
            metricsCollector.recordTableStats(taskId, result.getTableName(), result.getRowCount());
            metricsCollector.recordFileStats(taskId, result.getFilePath());
        }

        log.info("数据库卸载完成，共导出 {} 个表/查询", results.size());
    }

    private TableExportResult exportTable(Connection conn, String table, TaskExecutionContext context)
        throws Exception {
        // 生成文件名（支持标准命名）
        String fileName = generateFileName(context, table, false);
        Path file = prepareOutputFile(context, fileName);

        try {
            // 优先使用COPY TO STDOUT（无需服务器权限）
            long rowCount = exportTableWithCopyToStdout(conn, table, file);
            log.info("导出表 {} 完成，共 {} 行（使用COPY TO STDOUT）", table, rowCount);
            return new TableExportResult(table, file, rowCount);
        } catch (Exception e) {
            log.warn("COPY TO STDOUT失败，尝试COPY TO文件: {}", e.getMessage());
            try {
                // Fallback到COPY TO文件路径（需要服务器权限）
                long rowCount = exportTableWithCopyToFile(conn, table, file);
                log.info("导出表 {} 完成，共 {} 行（使用COPY TO文件）", table, rowCount);
                return new TableExportResult(table, file, rowCount);
            } catch (Exception e2) {
                log.error("COPY TO文件也失败，表 {} 导出失败", table, e2);
                throw e2;
            }
        }
    }

    private long exportTableWithCopyToStdout(Connection conn, String table, Path file) throws Exception {
        String copyToSql = String.format(
            "COPY %s TO STDOUT WITH (FORMAT text, DELIMITER E'\\x1E', ENCODING 'UTF-8', NULL '', HEADER false)",
            table
        );

        // PostgreSQL JDBC提供的CopyManager API
        CopyManager copyManager = new CopyManager((BaseConnection) conn);
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            long rows = copyManager.copyOut(copyToSql, fos);
            return rows;
        }
    }

    private long exportTableWithCopyToFile(Connection conn, String table, Path file) throws Exception {
        String copyToSql = String.format(
            "COPY %s TO '%s' WITH (FORMAT text, DELIMITER E'\\x1E', ENCODING 'UTF-8', NULL '', HEADER false)",
            table, file.toAbsolutePath().toString().replace("\\", "\\\\")
        );

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(copyToSql);
        }

        // 需要单独查询行数
        return getRowCount(conn, "SELECT COUNT(*) FROM " + table);
    }

    private TableExportResult exportSql(Connection conn, JobConfig.SqlItem sqlItem, TaskExecutionContext context)
        throws Exception {
        // 生成文件名（支持标准命名）
        String fileName = generateSqlFileName(context, sqlItem.getName(), true);
        Path file = prepareOutputFile(context, fileName);

        String copyToSql = String.format(
            "COPY (%s) TO STDOUT WITH (FORMAT text, DELIMITER E'\\x1E', ENCODING 'UTF-8', NULL '', HEADER false)",
            sqlItem.getSql()
        );

        log.info("按 SQL [{}] 导出到 {}", sqlItem.getName(), file);

        try {
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
                long rows = copyManager.copyOut(copyToSql, fos);
                log.info("SQL导出完成，共 {} 行", rows);
                return new TableExportResult(sqlItem.getName(), file, rows);
            }
        } catch (Exception e) {
            log.error("SQL导出失败: {}", sqlItem.getName(), e);
            throw e;
        }
    }

    private Path prepareOutputFile(TaskExecutionContext context, String fileName) throws IOException {
        JobConfig jobConfig = context.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && context.getGlobalConfig().getExtract() != null) {
            baseWorkDir = context.getGlobalConfig().getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        Path dir = Paths.get(baseWorkDir, jobName, context.getBatchNumber(), "data");
        Files.createDirectories(dir);
        return dir.resolve(fileName);
    }

    private long getRowCount(Connection conn, String countSql) {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countSql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (Exception e) {
            log.warn("获取行数失败: {}", e.getMessage());
        }
        return 0;
    }

    /**
     * 获取并发度配置
     */
    private int getConcurrency(TaskExecutionContext context) {
        // 优先使用作业级配置
        if (context.getJobConfig().getRuntime() != null
            && context.getJobConfig().getRuntime().getTableConcurrency() != null) {
            return context.getJobConfig().getRuntime().getTableConcurrency();
        }
        // 其次使用全局配置
        if (context.getGlobalConfig().getConcurrency() != null
            && context.getGlobalConfig().getConcurrency().getDefaultTableConcurrency() != null) {
            return context.getGlobalConfig().getConcurrency().getDefaultTableConcurrency();
        }
        // 默认串行
        return 1;
    }

    /**
     * 收集所有需要导出的任务
     */
    private List<ExportTask> collectExportTasks(JobConfig config, TaskExecutionContext ctx) {
        List<ExportTask> exportTasks = new ArrayList<>();

        if (config.getExtractTasks() == null || config.getExtractTasks().isEmpty()) {
            // 没有配置任务时，导出 demo_source 作为示例
            exportTasks.add(new ExportTask(ExportTaskType.TABLE, "demo_source", null, ctx));
        } else {
            for (JobConfig.ExtractTaskConfig task : config.getExtractTasks()) {
                // 全量表导出
                if (task.getTables() != null) {
                    for (String table : task.getTables()) {
                        exportTasks.add(new ExportTask(ExportTaskType.TABLE, table, null, ctx));
                    }
                }
                // 增量 SQL 导出
                if (task.getSqlList() != null) {
                    for (JobConfig.SqlItem sqlItem : task.getSqlList()) {
                        exportTasks.add(new ExportTask(ExportTaskType.SQL, null, sqlItem, ctx));
                    }
                }
            }
        }

        return exportTasks;
    }

    /**
     * 串行导出（单连接）
     */
    private List<TableExportResult> exportSerially(String url, JobConfig.ExtractDatabaseConfig db,
                                                    List<ExportTask> tasks) throws Exception {
        List<TableExportResult> results = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, db.getUser(), db.getPassword())) {
            for (ExportTask task : tasks) {
                TableExportResult result = executeExportTask(conn, task);
                if (result != null) {
                    results.add(result);
                }
            }
        }
        return results;
    }

    /**
     * 并发导出（连接池 + 线程池）
     */
    private List<TableExportResult> exportConcurrently(String url, JobConfig.ExtractDatabaseConfig db,
                                                        List<ExportTask> tasks, int concurrency) throws Exception {
        // 创建HikariCP连接池
        HikariDataSource dataSource = createDataSource(url, db, concurrency);
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<TableExportResult> results = new ArrayList<>();

        try {
            // 提交所有导出任务
            List<Future<TableExportResult>> futures = new ArrayList<>();
            for (ExportTask task : tasks) {
                Future<TableExportResult> future = executor.submit(new Callable<TableExportResult>() {
                    @Override
                    public TableExportResult call() throws Exception {
                        try (Connection conn = dataSource.getConnection()) {
                            return executeExportTask(conn, task);
                        }
                    }
                });
                futures.add(future);
            }

            // 等待所有任务完成并收集结果
            for (Future<TableExportResult> future : futures) {
                try {
                    TableExportResult result = future.get();
                    if (result != null) {
                        results.add(result);
                    }
                } catch (Exception e) {
                    log.error("导出任务失败", e);
                    throw new RuntimeException("并发导出失败", e);
                }
            }
        } finally {
            // 关闭线程池
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // 关闭连接池
            dataSource.close();
        }

        return results;
    }

    /**
     * 创建HikariCP连接池
     */
    private HikariDataSource createDataSource(String url, JobConfig.ExtractDatabaseConfig db, int concurrency) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(db.getUser());
        config.setPassword(db.getPassword());

        // 连接池大小：并发度 + 2个备用连接
        config.setMaximumPoolSize(concurrency + 2);
        config.setMinimumIdle(1);

        // 超时配置
        config.setConnectionTimeout(30000); // 30秒连接超时
        config.setIdleTimeout(600000);      // 10分钟空闲超时
        config.setMaxLifetime(1800000);     // 30分钟最大生命周期

        // 连接测试
        config.setConnectionTestQuery("SELECT 1");

        // 连接池名称
        config.setPoolName("kel-extract-pool");

        log.info("创建连接池: maxPoolSize={}, minIdle={}", config.getMaximumPoolSize(), config.getMinimumIdle());
        return new HikariDataSource(config);
    }

    /**
     * 执行单个导出任务
     */
    private TableExportResult executeExportTask(Connection conn, ExportTask task) throws Exception {
        if (task.type == ExportTaskType.TABLE) {
            return exportTable(conn, task.tableName, task.context);
        } else {
            return exportSql(conn, task.sqlItem, task.context);
        }
    }

    /**
     * 生成表文件名
     */
    private String generateFileName(TaskExecutionContext context, String tableName, boolean isIncremental) {
        if (context.getGlobalConfig().getFileNaming() != null) {
            int sequence = sequenceGenerator.getAndIncrement();
            return fileNamingService.generateStandardFileName(
                context.getGlobalConfig().getFileNaming(),
                tableName,
                sequence,
                isIncremental
            );
        }
        // 未配置标准命名，使用简单命名
        return tableName.replace('.', '_') + ".txt";
    }

    /**
     * 生成SQL文件名
     */
    private String generateSqlFileName(TaskExecutionContext context, String sqlName, boolean isIncremental) {
        if (context.getGlobalConfig().getFileNaming() != null) {
            int sequence = sequenceGenerator.getAndIncrement();
            return fileNamingService.generateSqlFileName(
                context.getGlobalConfig().getFileNaming(),
                sqlName,
                sequence,
                isIncremental
            );
        }
        // 未配置标准命名，使用简单命名
        return sqlName + ".txt";
    }

    /**
     * 导出任务类型
     */
    private enum ExportTaskType {
        TABLE, SQL
    }

    /**
     * 导出任务封装
     */
    private static class ExportTask {
        final ExportTaskType type;
        final String tableName;
        final JobConfig.SqlItem sqlItem;
        final TaskExecutionContext context;

        ExportTask(ExportTaskType type, String tableName, JobConfig.SqlItem sqlItem, TaskExecutionContext context) {
            this.type = type;
            this.tableName = tableName;
            this.sqlItem = sqlItem;
            this.context = context;
        }
    }

    /**
     * 表导出结果
     */
    @Data
    public static class TableExportResult {
        private final String tableName;
        private final Path filePath;
        private final long rowCount;
    }
}
