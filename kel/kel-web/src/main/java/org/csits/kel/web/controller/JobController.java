package org.csits.kel.web.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.JobConfigListItem;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.JobConfigService;
import org.csits.kel.server.service.ManualExportService;
import org.csits.kel.server.service.TaskExecutionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 作业管理API接口
 */
@Slf4j
@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobConfigService jobConfigService;
    private final TaskExecutionService taskExecutionService;
    private final ManualExportService manualExportService;

    /**
     * 触发卸载作业
     */
    @PostMapping("/{jobName}/extract")
    public ResponseEntity<Map<String, Object>> triggerExtract(@PathVariable String jobName) {
        try {
            log.info("触发卸载作业: {}", jobName);
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobName);
            TaskExecutionContext context = taskExecutionService.createContext(
                jobName, merged.getGlobalConfig(), merged.getJobConfig());

            // 异步执行
            new Thread(() -> {
                try {
                    taskExecutionService.executeExtract(context);
                } catch (Exception e) {
                    log.error("卸载作业执行失败", e);
                }
            }).start();

            Map<String, Object> result = new HashMap<>();
            result.put("taskId", context.getTaskId());
            result.put("batchNumber", context.getBatchNumber());
            result.put("message", "卸载作业已启动");

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("触发卸载作业失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 触发加载作业。sourceBatch 可选：指定要加载的卸载批次号（如 20260130_008），作为 load 的逆任务时必填。
     */
    @PostMapping("/{jobName}/load")
    public ResponseEntity<Map<String, Object>> triggerLoad(
            @PathVariable String jobName,
            @RequestParam(required = false) String sourceBatch) {
        try {
            log.info("触发加载作业: {}, sourceBatch={}", jobName, sourceBatch);
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobName);
            TaskExecutionContext context = taskExecutionService.createContext(
                jobName, merged.getGlobalConfig(), merged.getJobConfig(), sourceBatch);

            // 异步执行
            new Thread(() -> {
                try {
                    taskExecutionService.executeLoad(context);
                } catch (Exception e) {
                    log.error("加载作业执行失败", e);
                }
            }).start();

            Map<String, Object> result = new HashMap<>();
            result.put("taskId", context.getTaskId());
            result.put("batchNumber", context.getBatchNumber());
            result.put("message", "加载作业已启动");

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("触发加载作业失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 获取作业配置。path 为 job.name（与 config_key 一致）。
     */
    @GetMapping("/{jobName}/config")
    public ResponseEntity<JobConfig> getJobConfig(@PathVariable String jobName) {
        try {
            JobConfig config = jobConfigService.loadJobConfig(jobName);
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            log.error("获取作业配置失败", e);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 作业配置 key 列表（用于前端下拉等）
     */
    @GetMapping("/config-keys")
    public ResponseEntity<List<String>> listJobConfigKeys() {
        return ResponseEntity.ok(jobConfigService.listJobConfigKeys());
    }

    /**
     * 表级卸载用：仅 Kingbase 卸载作业（job.type=EXTRACT_KINGBASE），不含 file_extract。
     */
    @GetMapping("/config-keys/table-export")
    public ResponseEntity<List<String>> listConfigKeysForTableExport() {
        return ResponseEntity.ok(jobConfigService.listConfigKeysForTableExport());
    }

    /**
     * 表级加载用：仅 Kingbase 加载作业（job.type=KINGBASE_LOAD），不含 file_load。
     */
    @GetMapping("/config-keys/table-load")
    public ResponseEntity<List<String>> listConfigKeysForTableLoad() {
        return ResponseEntity.ok(jobConfigService.listConfigKeysForTableLoad());
    }

    /**
     * 作业配置列表（配置 Key、更新时间），用于作业配置页
     */
    @GetMapping("/configs")
    public ResponseEntity<List<JobConfigListItem>> listJobConfigs() {
        return ResponseEntity.ok(jobConfigService.listJobConfigs());
    }

    /**
     * 获取单条作业配置原始 YAML，供编辑框使用
     */
    @GetMapping("/configs/{configKey}")
    public ResponseEntity<Map<String, Object>> getJobConfigRaw(@PathVariable String configKey) {
        try {
            String yaml = jobConfigService.getRawContentYaml(configKey);
            Map<String, Object> body = new HashMap<>();
            body.put("contentYaml", yaml);
            return ResponseEntity.ok(body);
        } catch (IOException e) {
            log.error("获取配置失败: {}", configKey, e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 新增或覆盖作业配置。Body: { "contentYaml": "..." } 必填；可选 "configKey" 表示当前 key（更新时传入，须与 YAML 内 job.name 一致或用于重命名）。仅 database 模式可写。
     */
    @PostMapping("/configs")
    public ResponseEntity<Map<String, Object>> saveJobConfig(@RequestBody Map<String, String> body) {
        String contentYaml = body != null ? body.get("contentYaml") : null;
        if (contentYaml == null || contentYaml.trim().isEmpty()) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "contentYaml 不能为空");
            return ResponseEntity.badRequest().body(err);
        }
        String currentKey = body != null && body.containsKey("configKey") ? body.get("configKey") : null;
        if (currentKey != null && currentKey.trim().isEmpty()) {
            currentKey = null;
        }
        try {
            jobConfigService.saveJobConfig(currentKey, contentYaml);
            Map<String, Object> result = new HashMap<>();
            result.put("message", "保存成功");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("保存作业配置失败", e);
            Map<String, Object> err = new HashMap<>();
            err.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        }
    }

    /**
     * 更新作业配置 YAML。Body: { "contentYaml": "..." }。仅 database 模式可写。
     */
    @PutMapping("/configs/{configKey}")
    public ResponseEntity<Map<String, Object>> updateJobConfig(
            @PathVariable String configKey,
            @RequestBody Map<String, String> body) {
        String contentYaml = body != null ? body.get("contentYaml") : null;
        if (contentYaml == null) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "contentYaml 不能为空");
            return ResponseEntity.badRequest().body(err);
        }
        try {
            jobConfigService.saveJobConfig(configKey, contentYaml);
            Map<String, Object> result = new HashMap<>();
            result.put("message", "更新成功");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("更新作业配置失败", e);
            Map<String, Object> err = new HashMap<>();
            err.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        }
    }

    /**
     * 删除作业配置。仅 database 模式可写；禁止删除 __global__。
     */
    @DeleteMapping("/configs/{configKey}")
    public ResponseEntity<Map<String, Object>> deleteJobConfig(@PathVariable String configKey) {
        try {
            jobConfigService.deleteJobConfig(configKey);
            return ResponseEntity.noContent().build();
        } catch (IllegalArgumentException e) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        } catch (Exception e) {
            log.error("删除作业配置失败", e);
            Map<String, Object> err = new HashMap<>();
            err.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(err);
        }
    }

    /**
     * 从 classpath YAML 导入到 DB（用于首次初始化或批量导入）
     */
    @PostMapping("/admin/import-config")
    public ResponseEntity<Map<String, Object>> importConfigFromFiles() {
        try {
            jobConfigService.importFromFiles();
            Map<String, Object> result = new HashMap<>();
            result.put("message", "配置已导入");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("导入配置失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 存量迁移：使 YAML 内 job.name 与 config_key 一致（仅作业配置，不含 __global__）。
     */
    @PostMapping("/admin/migrate-job-name")
    public ResponseEntity<Map<String, Object>> migrateJobName() {
        try {
            int updated = jobConfigService.migrateJobNameToConfigKey();
            Map<String, Object> result = new HashMap<>();
            result.put("message", "迁移完成");
            result.put("updated", updated);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("迁移 job.name 失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 某作业可导出/可加载的表列表。extract 作业返回可导出表，load 作业返回可加载表。
     */
    @GetMapping("/{configKey}/tables")
    public ResponseEntity<java.util.List<String>> listTables(@PathVariable String configKey) {
        if (configKey != null && configKey.endsWith("_load")) {
            return ResponseEntity.ok(manualExportService.listLoadableTables(configKey));
        }
        return ResponseEntity.ok(manualExportService.listExportableTables(configKey));
    }

    /**
     * 人工表级导出（卸载）。策略按作业配置，不选全量/增量。
     */
    @PostMapping("/{configKey}/tables/{tableName}/export")
    public ResponseEntity<Map<String, Object>> triggerTableExport(
            @PathVariable String configKey,
            @PathVariable String tableName) {
        try {
            ManualExportService.ManualExportResult r = manualExportService.triggerTableExport(
                configKey, tableName);
            Map<String, Object> result = new HashMap<>();
            result.put("manualExportId", r.getManualExportId());
            result.put("taskId", r.getTaskId());
            result.put("batchNumber", r.getBatchNumber());
            result.put("message", "表级导出已启动（按作业配置执行）");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("触发表级导出失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 人工表级加载。按作业配置执行，sourceBatch 可选（不填则按 input_directory 取最新批次）。
     */
    @PostMapping("/{configKey}/tables/{tableName}/load")
    public ResponseEntity<Map<String, Object>> triggerTableLoad(
            @PathVariable String configKey,
            @PathVariable String tableName,
            @RequestParam(required = false) String sourceBatch) {
        try {
            ManualExportService.ManualExportResult r = manualExportService.triggerTableLoad(
                configKey, tableName, sourceBatch);
            Map<String, Object> result = new HashMap<>();
            result.put("manualExportId", r.getManualExportId());
            result.put("taskId", r.getTaskId());
            result.put("batchNumber", r.getBatchNumber());
            result.put("message", "表级加载已启动（按作业配置执行）");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("触发表级加载失败", e);
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 获取全局配置
     */
    @GetMapping("/global-config")
    public ResponseEntity<GlobalConfig> getGlobalConfig() {
        try {
            GlobalConfig config = jobConfigService.loadGlobalConfig();
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            log.error("获取全局配置失败", e);
            return ResponseEntity.notFound().build();
        }
    }
}
