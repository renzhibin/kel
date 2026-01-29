package org.csits.kel.dao;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryTaskExecutionRepositoryTest {

    private InMemoryTaskExecutionRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryTaskExecutionRepository();
    }

    @Test
    void create_assignsIncrementingIdAndSetsCreateTimeStartTime() {
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setJobCode("job1");
        entity.setBatchNumber("batch1");

        TaskExecutionEntity created = repository.create(entity);

        assertThat(created.getTaskId()).isEqualTo(1L);
        assertThat(created.getCreateTime()).isNotNull();
        assertThat(created.getStartTime()).isNotNull();
        assertThat(created.getJobCode()).isEqualTo("job1");
        assertThat(created.getBatchNumber()).isEqualTo("batch1");

        TaskExecutionEntity second = new TaskExecutionEntity();
        TaskExecutionEntity secondCreated = repository.create(second);
        assertThat(secondCreated.getTaskId()).isEqualTo(2L);
    }

    @Test
    void create_preservesProvidedCreateTimeAndStartTime() {
        LocalDateTime customCreate = LocalDateTime.of(2025, 1, 1, 10, 0);
        LocalDateTime customStart = LocalDateTime.of(2025, 1, 1, 10, 5);
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setCreateTime(customCreate);
        entity.setStartTime(customStart);

        TaskExecutionEntity created = repository.create(entity);

        assertThat(created.getCreateTime()).isEqualTo(customCreate);
        assertThat(created.getStartTime()).isEqualTo(customStart);
    }

    @Test
    void update_overwritesSameId() {
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setJobCode("job1");
        repository.create(entity);

        TaskExecutionEntity toUpdate = new TaskExecutionEntity();
        toUpdate.setTaskId(1L);
        toUpdate.setJobCode("job1-updated");
        toUpdate.setStatus(TaskExecutionStatus.SUCCESS);
        toUpdate.setProgress(100);
        repository.update(toUpdate);

        Optional<TaskExecutionEntity> found = repository.findById(1L);
        assertThat(found).isPresent();
        assertThat(found.get().getJobCode()).isEqualTo("job1-updated");
        assertThat(found.get().getStatus()).isEqualTo(TaskExecutionStatus.SUCCESS);
        assertThat(found.get().getProgress()).isEqualTo(100);
    }

    @Test
    void findById_returnsPresentWhenExists() {
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setJobCode("job1");
        repository.create(entity);

        Optional<TaskExecutionEntity> found = repository.findById(1L);
        assertThat(found).isPresent();
        assertThat(found.get().getTaskId()).isEqualTo(1L);
        assertThat(found.get().getJobCode()).isEqualTo("job1");
    }

    @Test
    void findById_returnsEmptyWhenNotExists() {
        Optional<TaskExecutionEntity> found = repository.findById(999L);
        assertThat(found).isEmpty();
    }
}
