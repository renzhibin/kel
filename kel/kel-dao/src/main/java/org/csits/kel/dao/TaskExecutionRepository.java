package org.csits.kel.dao;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * 任务执行仓储接口，支持内存和数据库两种实现。
 */
public interface TaskExecutionRepository {

    /**
     * 保存任务执行记录（新增或更新）
     */
    TaskExecutionEntity save(TaskExecutionEntity entity);

    /**
     * 根据ID查询任务执行记录
     */
    Optional<TaskExecutionEntity> findById(Long id);

    /**
     * 根据作业名查询任务执行记录列表
     */
    List<TaskExecutionEntity> findByJobName(String jobName);

    /**
     * 根据批次号查询任务执行记录
     */
    Optional<TaskExecutionEntity> findByBatchNumber(String batchNumber);

    /**
     * 查询所有任务执行记录
     */
    List<TaskExecutionEntity> findAll();

    /**
     * 删除任务执行记录
     */
    void deleteById(Long id);

    /**
     * 统计任务执行记录总数
     */
    long count();

    /**
     * 统计某时间区间内创建的任务数（startInclusive <= created_at < endExclusive）
     */
    long countByCreatedAtBetween(LocalDateTime startInclusive, LocalDateTime endExclusive);
}

