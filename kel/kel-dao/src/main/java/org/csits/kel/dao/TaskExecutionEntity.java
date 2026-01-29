package org.csits.kel.dao;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * 任务执行表实体，参照方案 3.1。
 */
@Data
public class TaskExecutionEntity {

    private Long taskId;

    private String jobCode;

    private String batchNumber;

    private String nodeName;

    private TaskExecutionStatus status;

    private String configSnapshot;

    private Integer progress;

    private String message;

    private String errorMessage;

    private LocalDateTime createTime;

    private LocalDateTime startTime;

    private LocalDateTime endTime;
}

