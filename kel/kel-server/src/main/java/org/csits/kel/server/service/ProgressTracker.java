package org.csits.kel.server.service;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.springframework.stereotype.Service;

/**
 * 任务进度跟踪服务
 * 提供细粒度的进度跟踪和阶段管理
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProgressTracker {

    private final TaskExecutionRepository taskExecutionRepository;

    /**
     * 阶段定义（卸载流程）
     */
    public enum ExtractStage {
        INIT("初始化", 0, 5),
        DISK_CHECK("磁盘检查", 5, 10),
        EXPORT("数据导出", 10, 50),
        MANIFEST("生成清单", 50, 60),
        COMPRESS("压缩打包", 60, 80),
        ENCRYPT("文件加密", 80, 90),
        DELIVER("文件交付", 90, 95),
        FINALIZE("完成清理", 95, 100);

        private final String description;
        private final int startProgress;
        private final int endProgress;

        ExtractStage(String description, int startProgress, int endProgress) {
            this.description = description;
            this.startProgress = startProgress;
            this.endProgress = endProgress;
        }

        public String getDescription() {
            return description;
        }

        public int getStartProgress() {
            return startProgress;
        }

        public int getEndProgress() {
            return endProgress;
        }
    }

    /**
     * 阶段定义（加载流程）
     */
    public enum LoadStage {
        INIT("初始化", 0, 5),
        DISK_CHECK("磁盘检查", 5, 10),
        DECRYPT("文件解密", 10, 20),
        UNPACK("解压文件", 20, 40),
        VALIDATE("校验数据", 40, 50),
        LOAD("数据加载", 50, 90),
        FINALIZE("完成清理", 90, 100);

        private final String description;
        private final int startProgress;
        private final int endProgress;

        LoadStage(String description, int startProgress, int endProgress) {
            this.description = description;
            this.startProgress = startProgress;
            this.endProgress = endProgress;
        }

        public String getDescription() {
            return description;
        }

        public int getStartProgress() {
            return startProgress;
        }

        public int getEndProgress() {
            return endProgress;
        }
    }

    /**
     * 更新任务进度（卸载流程）
     */
    public void updateExtractProgress(Long taskId, ExtractStage stage, int stageProgress) {
        int totalProgress = calculateProgress(stage.getStartProgress(), stage.getEndProgress(), stageProgress);
        updateProgress(taskId, stage.name(), stage.getDescription(), totalProgress);
    }

    /**
     * 更新任务进度（加载流程）
     */
    public void updateLoadProgress(Long taskId, LoadStage stage, int stageProgress) {
        int totalProgress = calculateProgress(stage.getStartProgress(), stage.getEndProgress(), stageProgress);
        updateProgress(taskId, stage.name(), stage.getDescription(), totalProgress);
    }

    /**
     * 更新任务进度（通用）
     */
    public void updateProgress(Long taskId, String stageName, String stageDescription, int progress) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity == null) {
            log.warn("任务不存在，无法更新进度: taskId={}", taskId);
            return;
        }

        entity.setCurrentStage(stageName);
        entity.setProgress(Math.min(100, Math.max(0, progress)));
        taskExecutionRepository.save(entity);

        log.debug("任务进度更新: taskId={}, stage={}, progress={}%",
            taskId, stageDescription, progress);
    }

    /**
     * 计算总进度
     */
    private int calculateProgress(int stageStart, int stageEnd, int stageProgress) {
        int stageRange = stageEnd - stageStart;
        return stageStart + (stageRange * stageProgress / 100);
    }

    /**
     * 获取任务进度信息
     */
    public Map<String, Object> getProgressInfo(Long taskId) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity == null) {
            return null;
        }

        Map<String, Object> info = new HashMap<>();
        info.put("taskId", taskId);
        info.put("status", entity.getStatus());
        info.put("progress", entity.getProgress());
        info.put("currentStage", entity.getCurrentStage());
        info.put("startTime", entity.getStartTime());
        info.put("endTime", entity.getEndTime());

        // 计算已用时间
        if (entity.getStartTime() != null) {
            java.time.LocalDateTime endTime = entity.getEndTime() != null
                ? entity.getEndTime()
                : java.time.LocalDateTime.now();
            long durationSeconds = java.time.Duration.between(entity.getStartTime(), endTime).getSeconds();
            info.put("durationSeconds", durationSeconds);
        }

        // 估算剩余时间（基于当前进度）
        if (entity.getProgress() > 0 && entity.getProgress() < 100 && entity.getStartTime() != null) {
            long elapsedSeconds = java.time.Duration.between(
                entity.getStartTime(),
                java.time.LocalDateTime.now()
            ).getSeconds();
            long estimatedTotalSeconds = elapsedSeconds * 100 / entity.getProgress();
            long remainingSeconds = estimatedTotalSeconds - elapsedSeconds;
            info.put("estimatedRemainingSeconds", Math.max(0, remainingSeconds));
        }

        return info;
    }

    /**
     * 重置任务进度
     */
    public void resetProgress(Long taskId) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity != null) {
            entity.setProgress(0);
            entity.setCurrentStage("INIT");
            taskExecutionRepository.save(entity);
            log.info("任务进度已重置: taskId={}", taskId);
        }
    }
}
