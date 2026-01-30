package org.csits.kel.server.dto;

import java.util.HashMap;
import java.util.Map;
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
     * 作业名（jobName，对应 job.name）。
     */
    private String jobName;

    /**
     * 合并后的全局配置。
     */
    private GlobalConfig globalConfig;

    /**
     * 作业配置。
     */
    private JobConfig jobConfig;

    /**
     * 上下文属性，用于在执行过程中传递数据。
     */
    private Map<String, Object> attributes = new HashMap<>();

    public TaskExecutionContext() {
    }

    public TaskExecutionContext(Long taskId, String batchNumber, String jobName,
                                GlobalConfig globalConfig, JobConfig jobConfig) {
        this.taskId = taskId;
        this.batchNumber = batchNumber;
        this.jobName = jobName;
        this.globalConfig = globalConfig;
        this.jobConfig = jobConfig;
    }

    /**
     * 设置属性
     */
    public void setAttribute(String key, Object value) {
        this.attributes.put(key, value);
    }

    /**
     * 获取属性
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key) {
        return (T) this.attributes.get(key);
    }
}

