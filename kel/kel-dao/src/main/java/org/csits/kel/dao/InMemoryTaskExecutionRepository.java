package org.csits.kel.dao;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Repository;

/**
 * 基于内存的任务执行仓储实现，便于后续替换为真实数据库。
 */
@Repository
public class InMemoryTaskExecutionRepository implements TaskExecutionRepository {

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final Map<Long, TaskExecutionEntity> store = new ConcurrentHashMap<>();

    @Override
    public TaskExecutionEntity create(TaskExecutionEntity entity) {
        long id = idGenerator.incrementAndGet();
        entity.setTaskId(id);
        LocalDateTime now = LocalDateTime.now();
        if (entity.getCreateTime() == null) {
            entity.setCreateTime(now);
        }
        if (entity.getStartTime() == null) {
            entity.setStartTime(now);
        }
        store.put(id, entity);
        return entity;
    }

    @Override
    public void update(TaskExecutionEntity entity) {
        store.put(entity.getTaskId(), entity);
    }

    @Override
    public Optional<TaskExecutionEntity> findById(Long taskId) {
        return Optional.ofNullable(store.get(taskId));
    }
}

