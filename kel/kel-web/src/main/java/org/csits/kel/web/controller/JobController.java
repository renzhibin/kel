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
    @PostMapping("/{jobCode}/extract")
    public ResponseEntity<Map<String, Object>> triggerExtract(@PathVariable String jobCode) {
        try {
            log.info("触发卸载作业: {}", jobCode);
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobCode, "extract");
            TaskExecutionContext context = taskExecutionService.createContext(
                jobCode, merged.getGlobalConfig(), merged.getJobConfig());

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
     * 触发加载作业
     */
    @PostMapping("/{jobCode}/load")
    public ResponseEntity<Map<String, Object>> triggerLoad(@PathVariable String jobCode) {
        try {
            log.info("触发加载作业: {}", jobCode);
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobCode, "load");
            TaskExecutionContext context = taskExecutionService.createContext(
                jobCode, merged.getGlobalConfig(), merged.getJobConfig());

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
     * 获取作业配置。jobCode 可为完整 key（如 demo_extract）或短编码，type 为 extract 或 load。
     */
    @GetMapping("/{jobCode}/config")
    public ResponseEntity<JobConfig> getJobConfig(
            @PathVariable String jobCode,
            @RequestParam(required = false, defaultValue = "extract") String type) {
        try {
            String configKey = jobCode.endsWith("_extract") || jobCode.endsWith("_load")
                ? jobCode : jobCode + "_" + type;
            JobConfig config = jobConfigService.loadJobConfig(configKey);
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
     * 新增或覆盖作业配置。Body: { "configKey": "xxx_extract", "contentYaml": "..." }。仅 database 模式可写。
     */
    @PostMapping("/configs")
    public ResponseEntity<Map<String, Object>> saveJobConfig(@RequestBody Map<String, String> body) {
        String configKey = body != null ? body.get("configKey") : null;
        String contentYaml = body != null ? body.get("contentYaml") : null;
        if (configKey == null || contentYaml == null) {
            Map<String, Object> err = new HashMap<>();
            err.put("error", "configKey 与 contentYaml 不能为空");
            return ResponseEntity.badRequest().body(err);
        }
        try {
            jobConfigService.saveJobConfig(configKey.trim(), contentYaml);
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
     * 某作业可导出的表列表（configKey 为完整 key，如 demo_extract）
     */
    @GetMapping("/{configKey}/tables")
    public ResponseEntity<java.util.List<String>> listExportableTables(@PathVariable String configKey) {
        return ResponseEntity.ok(manualExportService.listExportableTables(configKey));
    }

    /**
     * 人工表级导出。configKey 如 demo_extract，mode 为 full 或 incremental。
     */
    @PostMapping("/{configKey}/tables/{tableName}/export")
    public ResponseEntity<Map<String, Object>> triggerTableExport(
            @PathVariable String configKey,
            @PathVariable String tableName,
            @RequestParam(defaultValue = "full") String mode) {
        try {
            ManualExportService.ManualExportResult r = manualExportService.triggerTableExport(
                configKey, tableName, mode);
            Map<String, Object> result = new HashMap<>();
            result.put("manualExportId", r.getManualExportId());
            result.put("taskId", r.getTaskId());
            result.put("batchNumber", r.getBatchNumber());
            result.put("message", "表级导出已启动");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("触发表级导出失败", e);
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
