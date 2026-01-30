package org.csits.kel.server.plugin.file;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.filesystem.FileSystemManager;
import org.csits.kel.manager.plugin.LoadPlugin;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Component;

/**
 * 非结构化文件加载插件。解包后从工作目录 files/ 将文件复制到 target_directory，保持相对路径。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FileLoadPlugin implements LoadPlugin {

    private final FileSystemManager fileSystemManager;

    @Override
    public boolean supports(Object context) {
        if (!(context instanceof TaskExecutionContext)) {
            return false;
        }
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig.JobBasic job = ctx.getJobConfig().getJob();
        return job != null && job.getType() == JobType.FILE_LOAD;
    }

    @Override
    public void load(Object context) throws Exception {
        TaskExecutionContext ctx = (TaskExecutionContext) context;
        JobConfig config = ctx.getJobConfig();
        String targetDirStr = config.getTargetDirectory();
        if (targetDirStr == null || targetDirStr.isEmpty()) {
            log.warn("作业 {} 未配置 target_directory，跳过文件还原", ctx.getJobCode());
            return;
        }
        Path workDir = resolveWorkDir(ctx);
        Path filesDir = workDir.resolve("files");
        if (Files.notExists(filesDir) || !Files.isDirectory(filesDir)) {
            log.warn("作业 {} 工作目录下无 files 目录: {}", ctx.getJobCode(), filesDir);
            return;
        }
        Path targetRoot = Paths.get(targetDirStr);
        Files.createDirectories(targetRoot);
        List<Path> files = listFilesRecursively(filesDir);
        List<Map<String, String>> filePathMappings = new ArrayList<>();
        int copied = 0;
        for (Path file : files) {
            Path relative = filesDir.relativize(file);
            Path target = targetRoot.resolve(relative.toString());
            fileSystemManager.copyFile(file, target);
            Map<String, String> mapping = new LinkedHashMap<>();
            mapping.put("source", file.toAbsolutePath().toString());
            mapping.put("target", target.toAbsolutePath().toString());
            filePathMappings.add(mapping);
            copied++;
        }
        ctx.setAttribute("filePathMappings", filePathMappings);
        log.info("作业 {} 文件还原完成，共 {} 个文件到 {}", ctx.getJobCode(), copied, targetRoot);
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

    private List<Path> listFilesRecursively(Path dir) throws IOException {
        try (Stream<Path> stream = Files.walk(dir, FileVisitOption.FOLLOW_LINKS)) {
            return stream
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        }
    }
}
