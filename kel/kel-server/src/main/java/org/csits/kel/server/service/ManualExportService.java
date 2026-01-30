package org.csits.kel.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.ManualExportEntity;
import org.csits.kel.dao.ManualExportRepository;
import org.csits.kel.server.constants.ExtractType;
import org.csits.kel.server.constants.LoadMode;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Service;

/**
 * 人工表级导出/加载：按 jobName + tableName 触发，卸载/加载策略按作业配置执行，记录写入 kel.manual_export。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ManualExportService {

    private final JobConfigService jobConfigService;
    private final TaskExecutionService taskExecutionService;
    private final ManualExportRepository manualExportRepository;

    /**
     * 获取某作业配置中可导出的表名列表（从 extract_tasks 解析）。
     */
    public List<String> listExportableTables(String configKey) {
        try {
            JobConfig config = jobConfigService.loadJobConfig(configKey);
            if (config.getExtractTasks() == null) {
                return Collections.emptyList();
            }
            List<String> tables = new ArrayList<>();
            for (JobConfig.ExtractTaskConfig task : config.getExtractTasks()) {
                if (task.getTables() != null) {
                    tables.addAll(task.getTables());
                }
                if (task.getSqlList() != null) {
                    for (JobConfig.SqlItem item : task.getSqlList()) {
                        if (item.getName() != null) {
                            tables.add(item.getName());
                        }
                    }
                }
            }
            return tables;
        } catch (Exception e) {
            log.warn("listExportableTables failed for configKey={}: {}", configKey, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 获取某加载作业配置中可加载的表名列表（从 load_tasks 的 interfaceMapping value 与 sqlList name 解析）。
     */
    public List<String> listLoadableTables(String configKey) {
        try {
            JobConfig config = jobConfigService.loadJobConfig(configKey);
            if (config.getLoadTasks() == null) {
                return Collections.emptyList();
            }
            List<String> tables = new ArrayList<>();
            for (JobConfig.LoadTaskConfig task : config.getLoadTasks()) {
                if (task.getInterfaceMapping() != null) {
                    for (String targetTable : task.getInterfaceMapping().values()) {
                        if (targetTable != null && !tables.contains(targetTable)) {
                            tables.add(targetTable);
                        }
                    }
                }
                if (task.getSqlList() != null) {
                    for (JobConfig.SqlItem item : task.getSqlList()) {
                        if (item.getName() != null && !tables.contains(item.getName())) {
                            tables.add(item.getName());
                        }
                    }
                }
            }
            return tables;
        } catch (Exception e) {
            log.warn("listLoadableTables failed for configKey={}: {}", configKey, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 从作业配置中解析该表对应的卸载模式（FULL/INCREMENTAL），未配置则默认 FULL。
     */
    private String resolveExportModeFromConfig(JobConfig fullConfig, String tableName) {
        if (fullConfig.getExtractTasks() == null) return "FULL";
        for (JobConfig.ExtractTaskConfig task : fullConfig.getExtractTasks()) {
            boolean inTables = task.getTables() != null && task.getTables().contains(tableName);
            boolean inSqlList = false;
            if (task.getSqlList() != null) {
                for (JobConfig.SqlItem item : task.getSqlList()) {
                    if (tableName.equals(item.getName())) { inSqlList = true; break; }
                }
            }
            if (inTables || inSqlList) {
                return task.getType() == ExtractType.INCREMENTAL ? "INCREMENTAL" : "FULL";
            }
        }
        return "FULL";
    }

    /**
     * 触发表级导出。策略按作业配置（该表所在 extract_task 的 type），不传 mode。
     */
    public ManualExportResult triggerTableExport(String configKey, String tableName)
        throws IOException {
        GlobalConfig global = jobConfigService.loadGlobalConfig();
        JobConfig fullConfig = jobConfigService.loadJobConfig(configKey);
        String mode = resolveExportModeFromConfig(fullConfig, tableName);
        JobConfig singleTableConfig = buildSingleTableJobConfig(fullConfig, tableName, mode);

        TaskExecutionContext context = taskExecutionService.createContext(
            configKey, global, singleTableConfig);

        ManualExportEntity record = new ManualExportEntity();
        record.setType("EXPORT");
        record.setJobName(configKey);
        record.setTableName(tableName);
        record.setMode(mode);
        record.setSourceBatch(null);
        record.setStatus("RUNNING");
        record.setTaskId(context.getTaskId());
        record.setRequestedBy(null);
        manualExportRepository.save(record);

        final Long recordId = record.getId();
        final Long taskId = context.getTaskId();
        new Thread(() -> {
            try {
                taskExecutionService.executeExtract(context);
                updateManualExportStatus(recordId, taskId, "SUCCESS");
            } catch (Exception e) {
                log.error("表级导出失败 configKey={} table={}", configKey, tableName, e);
                updateManualExportStatus(recordId, taskId, "FAILED");
            }
        }).start();

        return new ManualExportResult(recordId, context.getTaskId(), context.getBatchNumber());
    }

    /**
     * 触发表级加载。按作业配置执行单表加载，sourceBatch 可选（不填则按 input_directory 取最新批次）。
     */
    public ManualExportResult triggerTableLoad(String configKey, String tableName, String sourceBatch)
        throws IOException {
        GlobalConfig global = jobConfigService.loadGlobalConfig();
        JobConfig fullConfig = jobConfigService.loadJobConfig(configKey);
        JobConfig singleTableConfig = buildSingleTableLoadConfig(fullConfig, tableName);

        TaskExecutionContext context = taskExecutionService.createContext(
            configKey, global, singleTableConfig, sourceBatch != null && !sourceBatch.trim().isEmpty() ? sourceBatch.trim() : null);

        ManualExportEntity record = new ManualExportEntity();
        record.setType("LOAD");
        record.setJobName(configKey);
        record.setTableName(tableName);
        record.setMode("CONFIG");
        record.setSourceBatch(context.getBatchNumber());
        record.setStatus("RUNNING");
        record.setTaskId(context.getTaskId());
        record.setRequestedBy(null);
        manualExportRepository.save(record);

        final Long recordId = record.getId();
        final Long taskId = context.getTaskId();
        new Thread(() -> {
            try {
                taskExecutionService.executeLoad(context);
                updateManualExportStatus(recordId, taskId, "SUCCESS");
            } catch (Exception e) {
                log.error("表级加载失败 configKey={} table={}", configKey, tableName, e);
                updateManualExportStatus(recordId, taskId, "FAILED");
            }
        }).start();

        return new ManualExportResult(recordId, context.getTaskId(), context.getBatchNumber());
    }

    public List<ManualExportEntity> listManualExports(String jobName, String tableName, int page, int size) {
        if (jobName != null && !jobName.isEmpty() && tableName != null && !tableName.isEmpty()) {
            return manualExportRepository.findByJobNameAndTableName(jobName, tableName);
        }
        if (jobName != null && !jobName.isEmpty()) {
            return manualExportRepository.findByJobName(jobName);
        }
        return manualExportRepository.findAll(page, size);
    }

    public long countManualExports() {
        return manualExportRepository.count();
    }

    private JobConfig buildSingleTableJobConfig(JobConfig source, String tableName, String mode) {
        JobConfig copy = new JobConfig();
        copy.setJob(source.getJob());
        copy.setExtractDatabase(source.getExtractDatabase());
        copy.setTargetDatabase(source.getTargetDatabase());
        copy.setWorkDir(source.getWorkDir());
        copy.setExchangeDir(source.getExchangeDir());
        copy.setRuntime(source.getRuntime());
        copy.setInputDirectory(source.getInputDirectory());
        copy.setExtractDirectory(source.getExtractDirectory());
        copy.setTargetDirectory(source.getTargetDirectory());

        JobConfig.ExtractTaskConfig task = new JobConfig.ExtractTaskConfig();
        task.setType("INCREMENTAL".equalsIgnoreCase(mode) ? ExtractType.INCREMENTAL : ExtractType.FULL);
        task.setTables(Collections.singletonList(tableName));
        copy.setExtractTasks(Collections.singletonList(task));
        return copy;
    }

    /**
     * 构造只含单表的加载配置：从 fullConfig 的 load_tasks 中保留仅包含 tableName 的映射。
     */
    private JobConfig buildSingleTableLoadConfig(JobConfig source, String tableName) {
        JobConfig copy = new JobConfig();
        copy.setJob(source.getJob());
        copy.setTargetDatabase(source.getTargetDatabase());
        copy.setWorkDir(source.getWorkDir());
        copy.setExchangeDir(source.getExchangeDir());
        copy.setRuntime(source.getRuntime());
        copy.setInputDirectory(source.getInputDirectory());
        copy.setExtractDirectory(source.getExtractDirectory());
        copy.setTargetDirectory(source.getTargetDirectory());

        if (source.getLoadTasks() == null || source.getLoadTasks().isEmpty()) {
            copy.setLoadTasks(Collections.emptyList());
            return copy;
        }
        List<JobConfig.LoadTaskConfig> singleTableTasks = new ArrayList<>();
        for (JobConfig.LoadTaskConfig task : source.getLoadTasks()) {
            Map<String, String> mapping = task.getInterfaceMapping();
            if (mapping == null) continue;
            Map<String, String> singleMapping = new LinkedHashMap<>();
            for (Map.Entry<String, String> e : mapping.entrySet()) {
                if (tableName.equals(e.getValue())) {
                    singleMapping.put(e.getKey(), e.getValue());
                    break;
                }
            }
            if (singleMapping.isEmpty()) continue;
            JobConfig.LoadTaskConfig singleTask = new JobConfig.LoadTaskConfig();
            singleTask.setType(task.getType() != null ? task.getType() : LoadMode.APPEND);
            singleTask.setInterfaceMapping(singleMapping);
            singleTask.setSqlList(null);
            singleTask.setEnableTransaction(task.getEnableTransaction());
            singleTableTasks.add(singleTask);
        }
        copy.setLoadTasks(singleTableTasks);
        return copy;
    }

    private void updateManualExportStatus(Long recordId, Long taskId, String status) {
        if (recordId == null) {
            return;
        }
        manualExportRepository.findById(recordId).ifPresent(record -> {
            record.setStatus(status);
            record.setTaskId(taskId);
            manualExportRepository.save(record);
        });
    }

    @lombok.Value
    public static class ManualExportResult {
        Long manualExportId;
        Long taskId;
        String batchNumber;
    }
}
