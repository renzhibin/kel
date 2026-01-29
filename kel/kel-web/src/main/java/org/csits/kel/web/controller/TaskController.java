package org.csits.kel.web.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.server.dto.TaskStatistics;
import org.csits.kel.server.service.MetricsCollector;
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

    /**
     * 查询所有任务
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> listTasks(
        @RequestParam(required = false) String jobCode,
        @RequestParam(required = false) String status,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {

        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findAll();

        // 过滤
        List<TaskExecutionEntity> filtered = allTasks.stream()
            .filter(t -> jobCode == null || jobCode.equals(t.getJobCode()))
            .filter(t -> status == null || status.equals(t.getStatus()))
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
     * 根据作业编码查询任务列表
     */
    @GetMapping("/by-job/{jobCode}")
    public ResponseEntity<List<TaskExecutionEntity>> getTasksByJobCode(@PathVariable String jobCode) {
        List<TaskExecutionEntity> tasks = taskExecutionRepository.findByJobCode(jobCode);
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
    public ResponseEntity<Map<String, Object>> getStats() {
        List<TaskExecutionEntity> allTasks = taskExecutionRepository.findAll();

        long total = allTasks.size();
        long running = allTasks.stream().filter(t -> "RUNNING".equals(t.getStatus())).count();
        long success = allTasks.stream().filter(t -> "SUCCESS".equals(t.getStatus())).count();
        long failed = allTasks.stream().filter(t -> "FAILED".equals(t.getStatus())).count();

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
}
