package org.csits.kel.dao;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * 任务执行表实体，参照方案 3.1。
 */
@Data
public class TaskExecutionEntity {

    private Long id;

    private String jobCode;

    private String batchNumber;

    private String nodeName;

    private String status;

    private String configSnapshot;

    private Integer progress;

    private String currentStage;

    private String errorMessage;

    private LocalDateTime startTime;

    private LocalDateTime endTime;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private String statistics;

    /**
     * 执行日志（原 task_execution_log 合并），JSON 数组，元素含 log_level、stage、message、created_at。
     */
    private String executionLog;

    // 兼容旧代码的getter/setter
    public Long getTaskId() {
        return id;
    }

    public void setTaskId(Long taskId) {
        this.id = taskId;
    }

    public LocalDateTime getCreateTime() {
        return createdAt;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createdAt = createTime;
    }

    public String getMessage() {
        return currentStage;
    }

    public void setMessage(String message) {
        this.currentStage = message;
    }
}

