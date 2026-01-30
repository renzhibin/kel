package org.csits.kel.server.plugin.kingbase;

import static org.assertj.core.api.Assertions.assertThat;

import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.FileNamingService;
import org.csits.kel.server.service.MetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KingbaseExtractPluginTest {

    @Mock
    private FileNamingService fileNamingService;
    @Mock
    private MetricsCollector metricsCollector;

    private KingbaseExtractPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new KingbaseExtractPlugin(fileNamingService, metricsCollector);
    }

    @Test
    void supports_returnsTrueWhenJobTypeIsExtractKingbase() {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.EXTRACT_KINGBASE);
        job.setName("test");
        jobConfig.setJob(job);
        ctx.setJobConfig(jobConfig);
        assertThat(plugin.supports(ctx)).isTrue();
    }

    @Test
    void supports_returnsFalseWhenJobTypeIsFileExtract() {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.FILE_EXTRACT);
        job.setName("test");
        jobConfig.setJob(job);
        ctx.setJobConfig(jobConfig);
        assertThat(plugin.supports(ctx)).isFalse();
    }

    @Test
    void supports_returnsFalseWhenContextNotTaskExecutionContext() {
        assertThat(plugin.supports("not a context")).isFalse();
    }

    @Test
    void supports_returnsFalseWhenJobIsNull() {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJob(null);
        ctx.setJobConfig(jobConfig);
        assertThat(plugin.supports(ctx)).isFalse();
    }
}
