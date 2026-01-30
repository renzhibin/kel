package org.csits.kel.server.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.ManualExportEntity;
import org.csits.kel.dao.ManualExportRepository;
import org.csits.kel.server.constants.ExtractType;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Service;

/**
 * 人工表级导出：按 jobCode + tableName + mode 触发表级导出，并写入 kel.manual_export。
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
     * 触发表级导出。configKey 如 demo_extract，mode 为 full 或 incremental。写入 manual_export 记录。
     */
    public ManualExportResult triggerTableExport(String configKey, String tableName, String mode)
        throws IOException {
        GlobalConfig global = jobConfigService.loadGlobalConfig();
        JobConfig fullConfig = jobConfigService.loadJobConfig(configKey);
        JobConfig singleTableConfig = buildSingleTableJobConfig(fullConfig, tableName, mode);

        TaskExecutionContext context = taskExecutionService.createContext(
            configKey, global, singleTableConfig);

        ManualExportEntity record = new ManualExportEntity();
        record.setJobCode(configKey);
        record.setTableName(tableName);
        record.setMode(mode == null ? "FULL" : mode.toUpperCase());
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

    public List<ManualExportEntity> listManualExports(String jobCode, String tableName, int page, int size) {
        if (jobCode != null && !jobCode.isEmpty() && tableName != null && !tableName.isEmpty()) {
            return manualExportRepository.findByJobCodeAndTableName(jobCode, tableName);
        }
        if (jobCode != null && !jobCode.isEmpty()) {
            return manualExportRepository.findByJobCode(jobCode);
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
        task.setType("incremental".equalsIgnoreCase(mode) ? ExtractType.INCREMENTAL : ExtractType.FULL);
        task.setTables(Collections.singletonList(tableName));
        copy.setExtractTasks(Collections.singletonList(task));
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
