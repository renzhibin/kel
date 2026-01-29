package org.csits.kel.web.controller;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.JobConfigService;
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

    /**
     * 触发卸载作业
     */
    @PostMapping("/{jobCode}/extract")
    public ResponseEntity<Map<String, Object>> triggerExtract(@PathVariable String jobCode) {
        try {
            log.info("触发卸载作业: {}", jobCode);
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobCode);
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
            JobConfigService.MergedResult merged = jobConfigService.loadMergedConfig(jobCode);
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
     * 获取作业配置
     */
    @GetMapping("/{jobCode}/config")
    public ResponseEntity<JobConfig> getJobConfig(@PathVariable String jobCode) {
        try {
            JobConfig config = jobConfigService.loadJobConfig(jobCode);
            return ResponseEntity.ok(config);
        } catch (Exception e) {
            log.error("获取作业配置失败", e);
            return ResponseEntity.notFound().build();
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
