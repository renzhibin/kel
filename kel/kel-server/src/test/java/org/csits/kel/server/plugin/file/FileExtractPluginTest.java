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

class FileExtractPluginTest {

    private FileExtractPlugin plugin;
    private LocalFileSystemManager fileSystemManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        fileSystemManager = new LocalFileSystemManager();
        plugin = new FileExtractPlugin(fileSystemManager);
    }

    @Test
    void supports_returnsTrueWhenJobTypeIsFileExtract() {
        TaskExecutionContext ctx = contextWithJobType(JobType.FILE_EXTRACT);
        assertThat(plugin.supports(ctx)).isTrue();
    }

    @Test
    void supports_returnsFalseWhenJobTypeIsExtractKingbase() {
        TaskExecutionContext ctx = contextWithJobType(JobType.EXTRACT_KINGBASE);
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

    @Test
    void extract_copiesMatchingFilesToWorkDirFiles() throws Exception {
        Path extractRoot = tempDir.resolve("extractRoot");
        Files.createDirectories(extractRoot);
        Path f1 = extractRoot.resolve("a.txt");
        Path f2 = extractRoot.resolve("b.txt");
        Path f3 = extractRoot.resolve("c.dat");
        Files.write(f1, "a".getBytes());
        Files.write(f2, "b".getBytes());
        Files.write(f3, "c".getBytes());

        Path workBase = tempDir.resolve("work");
        TaskExecutionContext ctx = contextForExtract(
            extractRoot.toString(), workBase.toString(), "*.txt");

        plugin.extract(ctx);

        Path filesDir = workBase.resolve("fileJob").resolve("batch1").resolve("files");
        assertThat(Files.exists(filesDir.resolve("a.txt"))).isTrue();
        assertThat(Files.exists(filesDir.resolve("b.txt"))).isTrue();
        assertThat(Files.exists(filesDir.resolve("c.dat"))).isFalse();
        assertThat(Files.readAllBytes(filesDir.resolve("a.txt"))).isEqualTo("a".getBytes());
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

    private TaskExecutionContext contextForExtract(String extractDir, String workDir, String pattern) {
        TaskExecutionContext ctx = new TaskExecutionContext();
        ctx.setJobCode("fileJob");
        ctx.setBatchNumber("batch1");
        GlobalConfig globalConfig = new GlobalConfig();
        ctx.setGlobalConfig(globalConfig);
        JobConfig jobConfig = new JobConfig();
        JobConfig.JobBasic job = new JobConfig.JobBasic();
        job.setType(JobType.FILE_EXTRACT);
        job.setName("fileJob");
        jobConfig.setJob(job);
        jobConfig.setExtractDirectory(extractDir);
        jobConfig.setWorkDir(workDir);
        JobConfig.ExtractTaskConfig task = new JobConfig.ExtractTaskConfig();
        JobConfig.FileAttribute attr = new JobConfig.FileAttribute();
        attr.setFilePattern(pattern);
        task.setAttribute(attr);
        jobConfig.setExtractTasks(Collections.singletonList(task));
        ctx.setJobConfig(jobConfig);
        return ctx;
    }
}
