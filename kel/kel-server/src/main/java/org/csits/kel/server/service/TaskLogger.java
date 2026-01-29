package org.csits.kel.server.service;

import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.dao.TaskExecutionStatus;
import org.springframework.stereotype.Component;

/**
 * 任务执行日志记录封装。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskLogger {

    private final TaskExecutionRepository taskExecutionRepository;

    public void logProgress(Long taskId, String phase, int progress, String message) {
        taskExecutionRepository.findById(taskId).ifPresent(entity -> {
            entity.setProgress(progress);
            entity.setCurrentStage(phase);
            taskExecutionRepository.save(entity);
        });
        log.info("[taskId={}] [{}] progress={}, message={}", taskId, phase, progress, message);
    }

    public void markSuccess(Long taskId, String message) {
        updateStatus(taskId, TaskExecutionStatus.SUCCESS, 100, message, null);
    }

    public void markFailed(Long taskId, String message, String error) {
        updateStatus(taskId, TaskExecutionStatus.FAILED, 0, message, error);
    }

    private void updateStatus(Long taskId, TaskExecutionStatus status, int progress,
                              String message, String error) {
        taskExecutionRepository.findById(taskId).ifPresent(entity -> {
            entity.setStatus(status.name());
            entity.setProgress(progress);
            entity.setCurrentStage(message);
            entity.setErrorMessage(error);
            if (status == TaskExecutionStatus.SUCCESS || status == TaskExecutionStatus.FAILED) {
                entity.setEndTime(LocalDateTime.now());
            }
            taskExecutionRepository.save(entity);
        });
        log.info("[taskId={}] status={}, progress={}, message={}, error={}",
            taskId, status, progress, message, error);
    }
}

