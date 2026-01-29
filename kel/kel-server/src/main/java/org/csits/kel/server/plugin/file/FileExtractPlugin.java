package org.csits.kel.server.plugin.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.filesystem.FileSystemManager;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Component;

/**
 * 非结构化文件卸载插件。按配置扫描源目录、按时间/规则筛选文件并复制到工作目录 files/。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileExtractPlugin implements ExtractPlugin {

    private final FileSystemManager fileSystemManager;

    @Override
    public boolean supports(Object context) {
        if (!(context instanceof TaskExecutionContext)) {
            return false;
        }
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig.JobBasic job = ctx.getJobConfig().getJob();
        return job != null && job.getType() == JobType.FILE_EXTRACT;
    }

    @Override
    public void extract(Object context) throws Exception {
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig config = ctx.getJobConfig();
        String extractDir = config.getExtractDirectory();
        if (extractDir == null || extractDir.isEmpty()) {
            log.warn("作业 {} 未配置 extract_directory，跳过文件采集", ctx.getJobCode());
            return;
        }
        Path root = Paths.get(extractDir);
        if (Files.notExists(root) || !Files.isDirectory(root)) {
            log.warn("作业 {} extract_directory 不存在或非目录: {}", ctx.getJobCode(), extractDir);
            return;
        }
        Path workDir = resolveWorkDir(ctx);
        Path filesDir = workDir.resolve("files");
        Files.createDirectories(filesDir);

        List<JobConfig.ExtractTaskConfig> tasks = config.getExtractTasks();
        if (tasks == null || tasks.isEmpty()) {
            log.info("作业 {} 未配置 extract_tasks，跳过文件采集", ctx.getJobCode());
            return;
        }
        for (JobConfig.ExtractTaskConfig task : tasks) {
            JobConfig.FileAttribute attr = task.getAttribute();
            String pattern = (attr != null && attr.getFilePattern() != null) ? attr.getFilePattern() : "*";
            List<Path> candidates = fileSystemManager.scanFiles(root, pattern);
            Instant cutoff = resolveTimeCutoff(attr);
            long sizeLimitBytes = resolveSizeLimitBytes(attr);
            int copied = 0;
            for (Path file : candidates) {
                if (cutoff != null && Files.getLastModifiedTime(file).toInstant().isBefore(cutoff)) {
                    continue;
                }
                if (sizeLimitBytes > 0 && Files.size(file) > sizeLimitBytes) {
                    continue;
                }
                Path relative = root.relativize(file);
                Path target = filesDir.resolve(relative.toString());
                fileSystemManager.copyFile(file, target);
                copied++;
            }
            log.info("作业 {} 文件采集完成，匹配 {} 个文件，复制 {} 个到 {}", ctx.getJobCode(), candidates.size(), copied, filesDir);
        }
    }

    private Path resolveWorkDir(TaskExecutionContext ctx) {
        JobConfig jobConfig = ctx.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && ctx.getGlobalConfig().getExtract() != null) {
            baseWorkDir = ctx.getGlobalConfig().getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        return Paths.get(baseWorkDir, jobName, ctx.getBatchNumber());
    }

    private Instant resolveTimeCutoff(JobConfig.FileAttribute attr) {
        if (attr == null || attr.getTimeRange() == null) {
            return null;
        }
        // LAST_1_DAY -> 24h ago
        if ("LAST_1_DAY".equals(attr.getTimeRange())) {
            return Instant.now().minusSeconds(24 * 3600);
        }
        if (attr.getTimeRange().startsWith("LAST_") && attr.getTimeRange().endsWith("_DAY")) {
            try {
                int days = Integer.parseInt(attr.getTimeRange().replace("LAST_", "").replace("_DAY", ""));
                return Instant.now().minusSeconds(days * 24L * 3600);
            } catch (NumberFormatException e) {
                return Instant.now().minusSeconds(24 * 3600);
            }
        }
        return null;
    }

    private long resolveSizeLimitBytes(JobConfig.FileAttribute attr) {
        if (attr == null || attr.getFileSizeLimitMb() == null || attr.getFileSizeLimitMb() <= 0) {
            return 0;
        }
        return attr.getFileSizeLimitMb() * 1024L * 1024L;
    }
}
