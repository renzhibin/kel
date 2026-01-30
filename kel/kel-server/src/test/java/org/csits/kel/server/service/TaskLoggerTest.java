package org.csits.kel.server.service;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.dao.TaskExecutionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class TaskLoggerTest {

    @Mock
    private TaskExecutionRepository taskExecutionRepository;

    private TaskLogger taskLogger;
    private TaskExecutionEntity entity;

    @BeforeEach
    void setUp() {
        taskLogger = new TaskLogger(taskExecutionRepository, new ObjectMapper());
        entity = new TaskExecutionEntity();
        entity.setTaskId(1L);
        entity.setJobName("job1");
        entity.setStatus(TaskExecutionStatus.RUNNING.name());
    }

    @Test
    void logProgress_updatesEntityProgressAndMessage() {
        when(taskExecutionRepository.findById(1L)).thenReturn(Optional.of(entity));

        taskLogger.logProgress(1L, "EXPORT", 50, "导出完成");

        ArgumentCaptor<TaskExecutionEntity> captor = ArgumentCaptor.forClass(TaskExecutionEntity.class);
        verify(taskExecutionRepository).save(captor.capture());
        TaskExecutionEntity updated = captor.getValue();
        assertThat(updated.getProgress()).isEqualTo(50);
        assertThat(updated.getCurrentStage()).isEqualTo("EXPORT");
    }

    @Test
    void markSuccess_setsStatusSuccessAndProgress100() {
        when(taskExecutionRepository.findById(1L)).thenReturn(Optional.of(entity));

        taskLogger.markSuccess(1L, "任务完成");

        ArgumentCaptor<TaskExecutionEntity> captor = ArgumentCaptor.forClass(TaskExecutionEntity.class);
        verify(taskExecutionRepository).save(captor.capture());
        TaskExecutionEntity updated = captor.getValue();
        assertThat(updated.getStatus()).isEqualTo("SUCCESS");
        assertThat(updated.getProgress()).isEqualTo(100);
        assertThat(updated.getCurrentStage()).isEqualTo("任务完成");
        assertThat(updated.getErrorMessage()).isNull();
    }

    @Test
    void markFailed_setsStatusFailedAndErrorMessage() {
        when(taskExecutionRepository.findById(1L)).thenReturn(Optional.of(entity));

        taskLogger.markFailed(1L, "失败", "连接超时");

        ArgumentCaptor<TaskExecutionEntity> captor = ArgumentCaptor.forClass(TaskExecutionEntity.class);
        verify(taskExecutionRepository).save(captor.capture());
        TaskExecutionEntity updated = captor.getValue();
        assertThat(updated.getStatus()).isEqualTo("FAILED");
        assertThat(updated.getProgress()).isEqualTo(0);
        assertThat(updated.getCurrentStage()).isEqualTo("失败");
        assertThat(updated.getErrorMessage()).isEqualTo("连接超时");
    }
}
