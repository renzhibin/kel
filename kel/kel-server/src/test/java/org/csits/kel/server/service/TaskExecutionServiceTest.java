package org.csits.kel.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.worker.core.ExtractPluginRegistry;
import org.csits.kel.server.worker.core.LoadPluginRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskExecutionServiceTest {

    @Mock
    private TaskExecutionRepository taskExecutionRepository;
    @Mock
    private org.csits.kel.manager.filesystem.FileSystemManager fileSystemManager;
    @Mock
    private org.csits.kel.manager.compression.CompressionManager compressionManager;
    @Mock
    private org.csits.kel.manager.security.SmCryptoManager smCryptoManager;
    @Mock
    private TaskLogger taskLogger;
    @Mock
    private ExtractPluginRegistry extractPluginRegistry;
    @Mock
    private LoadPluginRegistry loadPluginRegistry;
    @Mock
    private ManifestService manifestService;

    private TaskExecutionService service;

    @BeforeEach
    void setUp() {
        service = new TaskExecutionService(
            taskExecutionRepository,
            fileSystemManager,
            compressionManager,
            smCryptoManager,
            taskLogger,
            extractPluginRegistry,
            loadPluginRegistry,
            manifestService,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    @Test
    void createContext_returnsContextWithTaskIdBatchNumberJobCode() {
        when(taskExecutionRepository.countByCreatedAtBetween(any(LocalDateTime.class), any(LocalDateTime.class))).thenReturn(0L);
        doAnswer(inv -> {
            TaskExecutionEntity e = inv.getArgument(0);
            e.setId(100L);
            return e;
        }).when(taskExecutionRepository).save(any(TaskExecutionEntity.class));

        GlobalConfig globalConfig = new GlobalConfig();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setName("demo");
        jobConfig.setJob(job);

        TaskExecutionContext ctx = service.createContext("demo", globalConfig, jobConfig);

        String datePrefix = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
        assertThat(ctx.getTaskId()).isEqualTo(100L);
        assertThat(ctx.getBatchNumber()).isEqualTo(datePrefix + "_001");
        assertThat(ctx.getJobCode()).isEqualTo("demo");
        assertThat(ctx.getGlobalConfig()).isSameAs(globalConfig);
        assertThat(ctx.getJobConfig()).isSameAs(jobConfig);
    }
}
