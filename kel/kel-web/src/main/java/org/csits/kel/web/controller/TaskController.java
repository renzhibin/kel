package org.csits.kel.web.controller;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.ManifestMetadata;
import org.csits.kel.server.dto.TaskStatistics;
import org.csits.kel.server.service.JobConfigService;
import org.csits.kel.server.service.MetricsCollector;
import org.csits.kel.server.service.ManifestService;
import org.csits.kel.server.service.ProgressTracker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 任务管理API接口
 */
@Slf4j
@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskExecutionRepository taskExecutionRepository;
    private final MetricsCollector metricsCollector;
    private final ProgressTracker progressTracker;
    private final JobConfigService jobConfigService;
    private final ManifestService manifestService;

    /**
     * 查询所有任务
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listTasks(
        @RequestParam(required = false) String jobName,
        @RequestParam(required = false) String status,
        @RequestParam(defaultValue = "0") int days,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {

        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findAll();

        LocalDateTime since = (days > 0) ? LocalDateTime.now().minusDays(days) : null;

        // 过滤
        List<TaskExecutionEntity> filtered = allTasks.stream()
            .filter(t -> jobName == null || jobName.equals(t.getJobName()))
            .filter(t -> status == null || status.equals(t.getStatus()))
            .filter(t -> since == null || (t.getStartTime() != null && !t.getStartTime().isBefore(since)))
            .collect(Collectors.toList());

        // 分页
        int total = filtered.size();
        int start = page * size;
        int end = Math.min(start + size, total);
        List<TaskExecutionEntity> paged = filtered.subList(start, end);

        Map<String, Object> result = new HashMap<>();
        result.put("total", total);
        result.put("page", page);
        result.put("size", size);
        result.put("data", paged);

        return ResponseEntity.ok(result);
    }

    /**
     * 查询单个任务详情
     */
    @GetMapping("/{id}")
    public ResponseEntity<TaskExecutionEntity> getTask(@PathVariable Long id) {
        return taskExecutionRepository.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * 根据作业名查询任务列表
     */
    @GetMapping("/by-job/{jobName}")
    public ResponseEntity<List<TaskExecutionEntity>> getTasksByJobName(@PathVariable String jobName) {
        List<TaskExecutionEntity> tasks = taskExecutionRepository.findByJobName(jobName);
        return ResponseEntity.ok(tasks);
    }

    /**
     * 根据批次号查询任务
     */
    @GetMapping("/by-batch/{batchNumber}")
    public ResponseEntity<TaskExecutionEntity> getTaskByBatchNumber(@PathVariable String batchNumber) {
        return taskExecutionRepository.findByBatchNumber(batchNumber)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * 删除任务
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTask(@PathVariable Long id) {
        taskExecutionRepository.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    /**
     * 获取任务统计信息
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats(
        @RequestParam(defaultValue = "0") int days) {
        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findAll();

        LocalDateTime since = (days > 0) ? LocalDateTime.now().minusDays(days) : null;
        List<TaskExecutionEntity> filtered = allTasks;
        if (since != null) {
            filtered = allTasks.stream()
                .filter(t -> t.getStartTime() != null && !t.getStartTime().isBefore(since))
                .collect(Collectors.toList());
        }

        long total = filtered.size();
        long running = filtered.stream().filter(t -> "RUNNING".equals(t.getStatus())).count();
        long success = filtered.stream().filter(t -> "SUCCESS".equals(t.getStatus())).count();
        long failed = filtered.stream().filter(t -> "FAILED".equals(t.getStatus())).count();

        Map<String, Object> stats = new HashMap<>();
        stats.put("total", total);
        stats.put("running", running);
        stats.put("success", success);
        stats.put("failed", failed);

        return ResponseEntity.ok(stats);
    }

    /**
     * 获取任务详细统计信息
     */
    @GetMapping("/{id}/statistics")
    public ResponseEntity<TaskStatistics> getTaskStatistics(@PathVariable Long id) {
        TaskStatistics statistics = metricsCollector.loadTaskStatistics(id);
        if (statistics != null) {
            return ResponseEntity.ok(statistics);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 获取任务进度信息
     */
    @GetMapping("/{id}/progress")
    public ResponseEntity<Map<String, Object>> getTaskProgress(@PathVariable Long id) {
        Map<String, Object> progress = progressTracker.getProgressInfo(id);
        if (progress != null) {
            return ResponseEntity.ok(progress);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * 获取任务对应的 manifest 内容（从工作目录 manifest.json 读取）
     */
    @GetMapping("/{id}/manifest")
    public ResponseEntity<ManifestMetadata> getTaskManifest(@PathVariable Long id) {
        return taskExecutionRepository.findById(id)
            .flatMap(task -> {
                try {
                    GlobalConfig global = jobConfigService.loadGlobalConfig();
                    String workDir = global.getExtract() != null && global.getExtract().getWorkDir() != null
                        ? global.getExtract().getWorkDir()
                        : "work";
                    Path manifestPath = Paths.get(workDir, task.getJobName(), task.getBatchNumber(), "manifest.json");
                    if (!Files.isRegularFile(manifestPath)) {
                        return java.util.Optional.<ManifestMetadata>empty();
                    }
                    return java.util.Optional.of(manifestService.parseManifest(manifestPath));
                } catch (IOException e) {
                    log.warn("读取 manifest 失败: taskId={}", id, e);
                    return java.util.Optional.<ManifestMetadata>empty();
                }
            })
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
