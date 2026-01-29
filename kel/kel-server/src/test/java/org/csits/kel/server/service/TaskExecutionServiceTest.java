package org.csits.kel.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.manager.batch.BatchNumberGenerator;
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
    private BatchNumberGenerator batchNumberGenerator;
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
            batchNumberGenerator,
            extractPluginRegistry,
            loadPluginRegistry,
            manifestService
        );
    }

    @Test
    void createContext_returnsContextWithTaskIdBatchNumberJobCode() {
        when(batchNumberGenerator.nextBatchNumber()).thenReturn("20250129120000_001");
        doAnswer(inv -> {
            TaskExecutionEntity e = inv.getArgument(0);
            e.setTaskId(100L);
            return e;
        }).when(taskExecutionRepository).create(any(TaskExecutionEntity.class));

        GlobalConfig globalConfig = new GlobalConfig();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setName("demo");
        jobConfig.setJob(job);

        TaskExecutionContext ctx = service.createContext("demo", globalConfig, jobConfig);

        assertThat(ctx.getTaskId()).isEqualTo(100L);
        assertThat(ctx.getBatchNumber()).isEqualTo("20250129120000_001");
        assertThat(ctx.getJobCode()).isEqualTo("demo");
        assertThat(ctx.getGlobalConfig()).isSameAs(globalConfig);
        assertThat(ctx.getJobConfig()).isSameAs(jobConfig);
    }
}
