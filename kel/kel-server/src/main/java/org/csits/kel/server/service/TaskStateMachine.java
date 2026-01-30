package org.csits.kel.server.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.dao.TaskExecutionStatus;
import org.springframework.stereotype.Service;

/**
 * 任务状态机服务
 * 管理任务状态转换和验证
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskStateMachine {

    private final TaskExecutionRepository taskExecutionRepository;

    /**
     * 状态转换定义
     */
    private enum StateTransition {
        // 初始化 -> 运行中
        INIT_TO_RUNNING(null, TaskExecutionStatus.RUNNING),
        // 运行中 -> 成功
        RUNNING_TO_SUCCESS(TaskExecutionStatus.RUNNING, TaskExecutionStatus.SUCCESS),
        // 运行中 -> 失败
        RUNNING_TO_FAILED(TaskExecutionStatus.RUNNING, TaskExecutionStatus.FAILED),
        // 失败 -> 运行中（重试）
        FAILED_TO_RUNNING(TaskExecutionStatus.FAILED, TaskExecutionStatus.RUNNING),
        // 运行中 -> 取消
        RUNNING_TO_CANCELLED(TaskExecutionStatus.RUNNING, TaskExecutionStatus.CANCELLED);

        private final TaskExecutionStatus from;
        private final TaskExecutionStatus to;

        StateTransition(TaskExecutionStatus from, TaskExecutionStatus to) {
            this.from = from;
            this.to = to;
        }

        public boolean matches(String currentStatus, TaskExecutionStatus targetStatus) {
            TaskExecutionStatus current = currentStatus != null
                ? TaskExecutionStatus.valueOf(currentStatus)
                : null;
            return (this.from == null || this.from == current) && this.to == targetStatus;
        }
    }

    /**
     * 转换任务状态
     *
     * @param taskId 任务ID
     * @param targetStatus 目标状态
     * @param message 状态消息
     * @return 是否转换成功
     */
    public boolean transitionTo(Long taskId, TaskExecutionStatus targetStatus, String message) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity == null) {
            log.error("任务不存在: taskId={}", taskId);
            return false;
        }

        String currentStatus = entity.getStatus();

        // 幂等：当前已是目标状态则直接成功
        if (targetStatus.name().equals(currentStatus)) {
            log.debug("任务已是目标状态，跳过转换: taskId={}, status={}", taskId, currentStatus);
            return true;
        }

        // 验证状态转换是否合法
        if (!isValidTransition(currentStatus, targetStatus)) {
            log.error("非法的状态转换: taskId={}, from={}, to={}",
                taskId, currentStatus, targetStatus);
            return false;
        }

        // 执行状态转换
        entity.setStatus(targetStatus.name());
        if (message != null) {
            entity.setCurrentStage(message);
        }

        // 设置时间戳
        if (targetStatus == TaskExecutionStatus.RUNNING && entity.getStartTime() == null) {
            entity.setStartTime(java.time.LocalDateTime.now());
        } else if (targetStatus == TaskExecutionStatus.SUCCESS ||
                   targetStatus == TaskExecutionStatus.FAILED ||
                   targetStatus == TaskExecutionStatus.CANCELLED) {
            entity.setEndTime(java.time.LocalDateTime.now());
        }

        taskExecutionRepository.save(entity);
        log.info("任务状态转换: taskId={}, {} -> {}, message={}",
            taskId, currentStatus, targetStatus, message);

        return true;
    }

    /**
     * 验证状态转换是否合法
     */
    private boolean isValidTransition(String currentStatus, TaskExecutionStatus targetStatus) {
        for (StateTransition transition : StateTransition.values()) {
            if (transition.matches(currentStatus, targetStatus)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 标记任务为运行中
     */
    public boolean markRunning(Long taskId, String message) {
        return transitionTo(taskId, TaskExecutionStatus.RUNNING, message);
    }

    /**
     * 标记任务为成功
     */
    public boolean markSuccess(Long taskId, String message) {
        return transitionTo(taskId, TaskExecutionStatus.SUCCESS, message);
    }

    /**
     * 标记任务为失败
     */
    public boolean markFailed(Long taskId, String message, String errorMessage) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity != null) {
            entity.setErrorMessage(errorMessage);
            taskExecutionRepository.save(entity);
        }
        return transitionTo(taskId, TaskExecutionStatus.FAILED, message);
    }

    /**
     * 标记任务为取消
     */
    public boolean markCancelled(Long taskId, String message) {
        return transitionTo(taskId, TaskExecutionStatus.CANCELLED, message);
    }

    /**
     * 获取任务当前状态
     */
    public TaskExecutionStatus getCurrentStatus(Long taskId) {
        TaskExecutionEntity entity = taskExecutionRepository.findById(taskId).orElse(null);
        if (entity == null) {
            return null;
        }
        return TaskExecutionStatus.valueOf(entity.getStatus());
    }

    /**
     * 检查任务是否处于终态
     */
    public boolean isTerminalState(Long taskId) {
        TaskExecutionStatus status = getCurrentStatus(taskId);
        return status == TaskExecutionStatus.SUCCESS ||
               status == TaskExecutionStatus.FAILED ||
               status == TaskExecutionStatus.CANCELLED;
    }

    /**
     * 检查任务是否可以重试
     */
    public boolean canRetry(Long taskId) {
        TaskExecutionStatus status = getCurrentStatus(taskId);
        return status == TaskExecutionStatus.FAILED;
    }
}
