package org.csits.kel.server.dto;

import lombok.Data;

/**
 * 任务执行上下文，在执行卸载/加载时贯穿整个流程。
 */
@Data
public class TaskExecutionContext {

    /**
     * 任务实例 ID。
     */
    private Long taskId;

    /**
     * 批次号。
     */
    private String batchNumber;

    /**
     * 作业编码（jobCode/job.name）。
     */
    private String jobCode;

    /**
     * 合并后的全局配置。
     */
    private GlobalConfig globalConfig;

    /**
     * 作业配置。
     */
    private JobConfig jobConfig;

    public TaskExecutionContext() {
    }

    public TaskExecutionContext(Long taskId, String batchNumber, String jobCode,
                                GlobalConfig globalConfig, JobConfig jobConfig) {
        this.taskId = taskId;
        this.batchNumber = batchNumber;
        this.jobCode = jobCode;
        this.globalConfig = globalConfig;
        this.jobConfig = jobConfig;
    }
}

