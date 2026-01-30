package org.csits.kel.server.plugin.kingbase;

import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.FileNamingService;
import org.csits.kel.server.service.MetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发导出功能测试
 */
@ExtendWith(MockitoExtension.class)
class KingbaseExtractPluginConcurrencyTest {

    @Mock
    private FileNamingService fileNamingService;
    @Mock
    private MetricsCollector metricsCollector;

    private KingbaseExtractPlugin plugin;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        plugin = new KingbaseExtractPlugin(fileNamingService, metricsCollector);
    }

    @Test
    void testGetConcurrency_FromJobConfig() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        JobConfig.RuntimeConfig runtime = new JobConfig.RuntimeConfig();
        runtime.setTableConcurrency(5);
        context.getJobConfig().setRuntime(runtime);

        // 使用反射调用私有方法（仅用于测试）
        int concurrency = getConcurrencyViaReflection(context);

        // 验证：应该使用作业级配置
        assertEquals(5, concurrency);
    }

    @Test
    void testGetConcurrency_FromGlobalConfig() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        GlobalConfig.ConcurrencyConfig concurrencyConfig = new GlobalConfig.ConcurrencyConfig();
        concurrencyConfig.setDefaultTableConcurrency(3);
        context.getGlobalConfig().setConcurrency(concurrencyConfig);

        // 使用反射调用私有方法
        int concurrency = getConcurrencyViaReflection(context);

        // 验证：应该使用全局配置
        assertEquals(3, concurrency);
    }

    @Test
    void testGetConcurrency_DefaultValue() {
        // 准备测试数据
        TaskExecutionContext context = createContext();

        // 使用反射调用私有方法
        int concurrency = getConcurrencyViaReflection(context);

        // 验证：应该使用默认值1
        assertEquals(1, concurrency);
    }

    @Test
    void testGetConcurrency_JobConfigOverridesGlobal() {
        // 准备测试数据
        TaskExecutionContext context = createContext();

        // 设置全局配置
        GlobalConfig.ConcurrencyConfig concurrencyConfig = new GlobalConfig.ConcurrencyConfig();
        concurrencyConfig.setDefaultTableConcurrency(3);
        context.getGlobalConfig().setConcurrency(concurrencyConfig);

        // 设置作业级配置（应该覆盖全局配置）
        JobConfig.RuntimeConfig runtime = new JobConfig.RuntimeConfig();
        runtime.setTableConcurrency(8);
        context.getJobConfig().setRuntime(runtime);

        // 使用反射调用私有方法
        int concurrency = getConcurrencyViaReflection(context);

        // 验证：作业级配置应该优先
        assertEquals(8, concurrency);
    }

    @Test
    void testCollectExportTasks_WithTables() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        JobConfig.ExtractTaskConfig task = new JobConfig.ExtractTaskConfig();
        List<String> tables = new ArrayList<>();
        tables.add("table1");
        tables.add("table2");
        tables.add("table3");
        task.setTables(tables);

        List<JobConfig.ExtractTaskConfig> extractTasks = new ArrayList<>();
        extractTasks.add(task);
        context.getJobConfig().setExtractTasks(extractTasks);

        // 使用反射调用私有方法
        List<?> exportTasks = collectExportTasksViaReflection(context);

        // 验证：应该收集到3个表导出任务
        assertNotNull(exportTasks);
        assertEquals(3, exportTasks.size());
    }

    @Test
    void testCollectExportTasks_WithSql() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        JobConfig.ExtractTaskConfig task = new JobConfig.ExtractTaskConfig();

        List<JobConfig.SqlItem> sqlList = new ArrayList<>();
        JobConfig.SqlItem sql1 = new JobConfig.SqlItem();
        sql1.setName("query1");
        sql1.setSql("SELECT * FROM table1 WHERE id > 100");
        sqlList.add(sql1);

        JobConfig.SqlItem sql2 = new JobConfig.SqlItem();
        sql2.setName("query2");
        sql2.setSql("SELECT * FROM table2 WHERE status = 'active'");
        sqlList.add(sql2);

        task.setSqlList(sqlList);

        List<JobConfig.ExtractTaskConfig> extractTasks = new ArrayList<>();
        extractTasks.add(task);
        context.getJobConfig().setExtractTasks(extractTasks);

        // 使用反射调用私有方法
        List<?> exportTasks = collectExportTasksViaReflection(context);

        // 验证：应该收集到2个SQL导出任务
        assertNotNull(exportTasks);
        assertEquals(2, exportTasks.size());
    }

    @Test
    void testCollectExportTasks_Mixed() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        JobConfig.ExtractTaskConfig task = new JobConfig.ExtractTaskConfig();

        // 添加表
        List<String> tables = new ArrayList<>();
        tables.add("table1");
        tables.add("table2");
        task.setTables(tables);

        // 添加SQL
        List<JobConfig.SqlItem> sqlList = new ArrayList<>();
        JobConfig.SqlItem sql1 = new JobConfig.SqlItem();
        sql1.setName("query1");
        sql1.setSql("SELECT * FROM table3");
        sqlList.add(sql1);
        task.setSqlList(sqlList);

        List<JobConfig.ExtractTaskConfig> extractTasks = new ArrayList<>();
        extractTasks.add(task);
        context.getJobConfig().setExtractTasks(extractTasks);

        // 使用反射调用私有方法
        List<?> exportTasks = collectExportTasksViaReflection(context);

        // 验证：应该收集到3个任务（2个表 + 1个SQL）
        assertNotNull(exportTasks);
        assertEquals(3, exportTasks.size());
    }

    @Test
    void testCollectExportTasks_Empty() {
        // 准备测试数据
        TaskExecutionContext context = createContext();
        context.getJobConfig().setExtractTasks(new ArrayList<>());

        // 使用反射调用私有方法
        List<?> exportTasks = collectExportTasksViaReflection(context);

        // 验证：应该返回demo_source作为默认任务
        assertNotNull(exportTasks);
        assertEquals(1, exportTasks.size());
    }

    // ========== 辅助方法 ==========

    private TaskExecutionContext createContext() {
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic jobBasic = new JobConfig.JobBasic();
        jobBasic.setName("test-job");
        jobConfig.setJob(jobBasic);
        jobConfig.setWorkDir(tempDir.toString());

        GlobalConfig globalConfig = new GlobalConfig();

        TaskExecutionContext context = new TaskExecutionContext(
            1L,
            "20260129000001",
            "test-job",
            globalConfig,
            jobConfig
        );

        return context;
    }

    /**
     * 使用反射调用私有方法 getConcurrency
     */
    private int getConcurrencyViaReflection(TaskExecutionContext context) {
        try {
            java.lang.reflect.Method method = KingbaseExtractPlugin.class
                .getDeclaredMethod("getConcurrency", TaskExecutionContext.class);
            method.setAccessible(true);
            return (int) method.invoke(plugin, context);
        } catch (Exception e) {
            throw new RuntimeException("反射调用失败", e);
        }
    }

    /**
     * 使用反射调用私有方法 collectExportTasks
     */
    private List<?> collectExportTasksViaReflection(TaskExecutionContext context) {
        try {
            java.lang.reflect.Method method = KingbaseExtractPlugin.class
                .getDeclaredMethod("collectExportTasks", JobConfig.class, TaskExecutionContext.class);
            method.setAccessible(true);
            return (List<?>) method.invoke(plugin, context.getJobConfig(), context);
        } catch (Exception e) {
            throw new RuntimeException("反射调用失败", e);
        }
    }
}
