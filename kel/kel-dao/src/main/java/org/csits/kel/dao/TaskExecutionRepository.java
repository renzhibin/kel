package org.csits.kel.dao;

import java.util.Optional;

/**
 * 任务执行仓储接口，当前提供内存实现，后续可替换为数据库实现。
 */
public interface TaskExecutionRepository {

    TaskExecutionEntity create(TaskExecutionEntity entity);

    void update(TaskExecutionEntity entity);

    Optional<TaskExecutionEntity> findById(Long taskId);
}

