package org.csits.kel.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.server.dto.StageMetrics;
import org.csits.kel.server.dto.TaskStatistics;
import org.csits.kel.server.serializer.LocalDateTimeDeserializer;
import org.springframework.stereotype.Service;

/**
 * 指标收集服务
 * 负责收集和持久化任务执行的各项指标
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MetricsCollector {

    private final TaskExecutionRepository taskExecutionRepository;
    private final ObjectMapper objectMapper = createStatisticsObjectMapper();

    private static ObjectMapper createStatisticsObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        SimpleModule module = new SimpleModule();
        module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());
        mapper.registerModule(module);
        return mapper;
    }

    // 内存中的指标缓存
    private final Map<Long, TaskStatistics> metricsCache = new ConcurrentHashMap<>();
    private final Map<Long, Map<String, LocalDateTime>> stageStartTimes = new ConcurrentHashMap<>();

    /**
     * 初始化任务统计
     */
    public void initTaskStatistics(Long taskId, String batchNumber) {
        TaskStatistics stats = new TaskStatistics();
        stats.setTaskId(taskId);
        stats.setBatchNumber(batchNumber);
        metricsCache.put(taskId, stats);
        stageStartTimes.put(taskId, new ConcurrentHashMap<>());
        log.debug("初始化任务统计: taskId={}", taskId);
    }

    /**
     * 记录阶段开始
     */
    public void recordStageStart(Long taskId, String stageName) {
        Map<String, LocalDateTime> stages = stageStartTimes.get(taskId);
        if (stages != null) {
            stages.put(stageName, LocalDateTime.now());
            log.debug("记录阶段开始: taskId={}, stage={}", taskId, stageName);
        }
    }

    /**
     * 记录阶段结束
     */
    public void recordStageEnd(Long taskId, String stageName, String status, String message) {
        Map<String, LocalDateTime> stages = stageStartTimes.get(taskId);
        TaskStatistics stats = metricsCache.get(taskId);

        if (stages != null && stats != null) {
            LocalDateTime startTime = stages.get(stageName);
            LocalDateTime endTime = LocalDateTime.now();

            if (startTime != null) {
                long durationMs = java.time.Duration.between(startTime, endTime).toMillis();

                StageMetrics metrics = StageMetrics.builder()
                    .stageName(stageName)
                    .startTime(startTime)
                    .endTime(endTime)
                    .durationMs(durationMs)
                    .status(status)
                    .message(message)
                    .build();

                stats.addStageMetrics(metrics);
                log.info("记录阶段结束: taskId={}, stage={}, duration={}ms", taskId, stageName, durationMs);
            }
        }
    }

    /**
     * 记录表统计
     */
    public void recordTableStats(Long taskId, String tableName, long rowCount) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null) {
            stats.addTableStats(tableName, rowCount);
            log.debug("记录表统计: taskId={}, table={}, rows={}", taskId, tableName, rowCount);
        }
    }

    /**
     * 记录文件统计
     */
    public void recordFileStats(Long taskId, Path file) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null && Files.exists(file)) {
            try {
                long size = Files.size(file);
                stats.setTotalFiles(stats.getTotalFiles() + 1);
                stats.setTotalFileSize(stats.getTotalFileSize() + size);
                log.debug("记录文件统计: taskId={}, file={}, size={}", taskId, file.getFileName(), size);
            } catch (IOException e) {
                log.warn("获取文件大小失败: {}", file, e);
            }
        }
    }

    /**
     * 记录压缩统计
     */
    public void recordCompressionStats(Long taskId, long originalSize, long compressedSize) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null) {
            stats.setOriginalSize(originalSize);
            stats.setCompressedSize(compressedSize);
            stats.calculateCompressionRatio();
            log.info("记录压缩统计: taskId={}, original={}bytes, compressed={}bytes, ratio={}",
                taskId, originalSize, compressedSize, stats.getCompressionRatio());
        }
    }

    /**
     * 记录分片统计
     */
    public void recordSplitStats(Long taskId, List<Path> splitFiles) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null) {
            stats.setSplitCount(splitFiles.size());
            for (Path file : splitFiles) {
                stats.getSplitFiles().add(file.getFileName().toString());
            }
            log.info("记录分片统计: taskId={}, splitCount={}", taskId, splitFiles.size());
        }
    }

    /**
     * 记录加密统计
     */
    public void recordEncryptionStats(Long taskId, long durationMs) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null) {
            stats.setEncrypted(true);
            stats.setEncryptionDurationMs(durationMs);
            log.info("记录加密统计: taskId={}, duration={}ms", taskId, durationMs);
        }
    }

    /**
     * 获取任务统计
     */
    public TaskStatistics getTaskStatistics(Long taskId) {
        return metricsCache.get(taskId);
    }

    /**
     * 持久化任务统计
     */
    public void persistTaskStatistics(Long taskId) {
        TaskStatistics stats = metricsCache.get(taskId);
        if (stats != null) {
            try {
                // 计算总耗时
                long totalDuration = stats.getStageMetrics().stream()
                    .mapToLong(StageMetrics::getDurationMs)
                    .sum();
                stats.setTotalDurationMs(totalDuration);

                // 序列化为JSON并保存到数据库
                String statsJson = objectMapper.writeValueAsString(stats);

                TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
                if (entity != null) {
                    entity.setStatistics(statsJson);
                    taskExecutionRepository.save(entity);
                    log.info("持久化任务统计: taskId={}", taskId);
                }
            } catch (Exception e) {
                log.error("持久化任务统计失败: taskId={}", taskId, e);
            }
        }
    }

    /**
     * 清理任务统计缓存
     */
    public void clearTaskStatistics(Long taskId) {
        metricsCache.remove(taskId);
        stageStartTimes.remove(taskId);
        log.debug("清理任务统计缓存: taskId={}", taskId);
    }

    /**
     * 从数据库加载任务统计
     */
    public TaskStatistics loadTaskStatistics(Long taskId) {
        try {
            TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
            if (entity != null && entity.getStatistics() != null) {
                return objectMapper.readValue(entity.getStatistics(), TaskStatistics.class);
            }
        } catch (Exception e) {
            log.error("加载任务统计失败: taskId={}", taskId, e);
        }
        return null;
    }
}
