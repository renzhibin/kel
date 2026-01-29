package org.csits.kel.dao;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;

/**
 * 基于内存的任务执行仓储实现，便于后续替换为真实数据库。
 */
@Repository
@ConditionalOnProperty(name = "kel.persistence.type", havingValue = "memory")
public class InMemoryTaskExecutionRepository implements TaskExecutionRepository {

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final Map<Long, TaskExecutionEntity> store = new ConcurrentHashMap<>();

    @Override
    public TaskExecutionEntity save(TaskExecutionEntity entity) {
        if (entity.getId() == null) {
            long id = idGenerator.incrementAndGet();
            entity.setId(id);
            LocalDateTime now = LocalDateTime.now();
            if (entity.getCreatedAt() == null) {
                entity.setCreatedAt(now);
            }
            if (entity.getStartTime() == null) {
                entity.setStartTime(now);
            }
        }
        entity.setUpdatedAt(LocalDateTime.now());
        store.put(entity.getId(), entity);
        return entity;
    }

    @Override
    public Optional<TaskExecutionEntity> findById(Long id) {
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public List<TaskExecutionEntity> findByJobCode(String jobCode) {
        return store.values().stream()
            .filter(e -> jobCode.equals(e.getJobCode()))
            .sorted((a, b) -> b.getCreatedAt().compareTo(a.getCreatedAt()))
            .collect(Collectors.toList());
    }

    @Override
    public Optional<TaskExecutionEntity> findByBatchNumber(String batchNumber) {
        return store.values().stream()
            .filter(e -> batchNumber.equals(e.getBatchNumber()))
            .findFirst();
    }

    @Override
    public List<TaskExecutionEntity> findAll() {
        return new ArrayList<>(store.values());
    }

    @Override
    public void deleteById(Long id) {
        store.remove(id);
    }

    @Override
    public long count() {
        return store.size();
    }
}

