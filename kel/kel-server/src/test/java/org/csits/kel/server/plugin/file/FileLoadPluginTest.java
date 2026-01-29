package org.csits.kel.server.plugin.file;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.csits.kel.manager.filesystem.LocalFileSystemManager;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileLoadPluginTest {

    private FileLoadPlugin plugin;
    private LocalFileSystemManager fileSystemManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        fileSystemManager = new LocalFileSystemManager();
        plugin = new FileLoadPlugin(fileSystemManager);
    }

    @Test
    void supports_returnsTrueWhenJobTypeIsFileLoad() {
        TaskExecutionContext ctx = contextWithJobType(JobType.FILE_LOAD);
        assertThat(plugin.supports(ctx)).isTrue();
    }

    @Test
    void supports_returnsFalseWhenJobTypeIsKingbaseLoad() {
        TaskExecutionContext ctx = contextWithJobType(JobType.KINGBASE_LOAD);
        assertThat(plugin.supports(ctx)).isFalse();
    }

    @Test
    void supports_returnsFalseWhenContextNotTaskExecutionContext() {
        assertThat(plugin.supports("not a context")).isFalse();
    }

    @Test
    void load_copiesFilesFromWorkDirFilesToTargetDirectory() throws Exception {
        Path workDir = tempDir.resolve("work").resolve("loadJob").resolve("batch1");
        Path filesDir = workDir.resolve("files");
        Files.createDirectories(filesDir);
        Path f1 = filesDir.resolve("x.txt");
        Path sub = filesDir.resolve("sub");
        Files.createDirectories(sub);
        Path f2 = sub.resolve("y.txt");
        Files.write(f1, "x".getBytes());
        Files.write(f2, "y".getBytes());

        Path targetRoot = tempDir.resolve("target");
        TaskExecutionContext ctx = contextForLoad(workDir.getParent().getParent().toString(), targetRoot.toString());

        plugin.load(ctx);

        assertThat(Files.exists(targetRoot.resolve("x.txt"))).isTrue();
        assertThat(Files.exists(targetRoot.resolve("sub").resolve("y.txt"))).isTrue();
        assertThat(Files.readAllBytes(targetRoot.resolve("x.txt"))).isEqualTo("x".getBytes());
        assertThat(Files.readAllBytes(targetRoot.resolve("sub").resolve("y.txt"))).isEqualTo("y".getBytes());
    }

    private TaskExecutionContext contextWithJobType(JobType type) {
        TaskExecutionContext ctx = new TaskExecutionContext();
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(type);
        job.setName("test");
        jobConfig.setJob(job);
        ctx.setJobConfig(jobConfig);
        return ctx;
    }

    private TaskExecutionContext contextForLoad(String workBaseDir, String targetDir) {
        TaskExecutionContext ctx = new TaskExecutionContext();
        ctx.setJobCode("loadJob");
        ctx.setBatchNumber("batch1");
        GlobalConfig globalConfig = new GlobalConfig();
        ctx.setGlobalConfig(globalConfig);
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.FILE_LOAD);
        job.setName("loadJob");
        jobConfig.setJob(job);
        jobConfig.setWorkDir(workBaseDir);
        jobConfig.setTargetDirectory(targetDir);
        ctx.setJobConfig(jobConfig);
        return ctx;
    }
}
