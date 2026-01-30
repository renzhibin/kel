package org.csits.kel.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.dao.TaskExecutionStatus;
import org.springframework.stereotype.Component;

/**
 * 任务执行日志记录封装。进度与阶段写入 task_execution，历史日志追加到 execution_log（JSON 数组）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskLogger {

    private static final TypeReference<List<Map<String, Object>>> LOG_LIST_TYPE = new TypeReference<List<Map<String, Object>>>() {};

    private final TaskExecutionRepository taskExecutionRepository;
    private final ObjectMapper objectMapper;

    public void logProgress(Long taskId, String phase, int progress, String message) {
        taskExecutionRepository.findById(taskId).ifPresent(entity -> {
            entity.setProgress(progress);
            entity.setCurrentStage(phase);
            appendExecutionLog(entity, "INFO", phase, message);
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

    private void appendExecutionLog(TaskExecutionEntity entity, String logLevel, String stage, String message) {
        String raw = entity.getExecutionLog();
        if (raw == null || raw.trim().isEmpty()) {
            raw = "[]";
        }
        try {
            List<Map<String, Object>> list = objectMapper.readValue(raw, LOG_LIST_TYPE);
            if (list == null) {
                list = new ArrayList<>();
            }
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("log_level", logLevel);
            entry.put("stage", stage);
            entry.put("message", message);
            entry.put("created_at", LocalDateTime.now().toString());
            list.add(entry);
            entity.setExecutionLog(objectMapper.writeValueAsString(list));
        } catch (Exception e) {
            log.warn("appendExecutionLog parse/write failed, skip: {}", e.getMessage());
        }
    }
}

