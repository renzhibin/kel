package org.csits.kel.server.plugin.kingbase;

import static org.assertj.core.api.Assertions.assertThat;

import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KingbaseLoadPluginTest {

    private KingbaseLoadPlugin plugin;

    @BeforeEach
    void setUp() {
        plugin = new KingbaseLoadPlugin();
    }

    @Test
    void supports_returnsTrueWhenJobTypeIsKingbaseLoad() {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.KINGBASE_LOAD);
        job.setName("test");
        jobConfig.setJob(job);
        ctx.setJobConfig(jobConfig);
        assertThat(plugin.supports(ctx)).isTrue();
    }

    @Test
    void supports_returnsFalseWhenJobTypeIsFileLoad() {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.FILE_LOAD);
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
