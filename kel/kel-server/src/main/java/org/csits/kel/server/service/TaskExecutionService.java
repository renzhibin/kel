package org.csits.kel.server.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.TaskExecutionEntity;
import org.csits.kel.dao.TaskExecutionRepository;
import org.csits.kel.dao.TaskExecutionStatus;
import org.csits.kel.manager.batch.BatchNumberGenerator;
import org.csits.kel.manager.compression.CompressionManager;
import org.csits.kel.manager.filesystem.FileSystemManager;
import org.csits.kel.manager.security.SmCryptoManager;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.manager.plugin.LoadPlugin;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.worker.core.ExtractPluginRegistry;
import org.csits.kel.server.worker.core.LoadPluginRegistry;
import org.springframework.stereotype.Service;

/**
 * 任务执行主流程（卸载与加载），当前实现一个简化版本：
 * - 创建工作目录
 * - （预留）结构化数据导出/加载
 * - 打包压缩与分片
 * - 调用占位的国密加密
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskExecutionService {

    private final TaskExecutionRepository taskExecutionRepository;
    private final FileSystemManager fileSystemManager;
    private final CompressionManager compressionManager;
    private final SmCryptoManager smCryptoManager;
    private final TaskLogger taskLogger;
    private final BatchNumberGenerator batchNumberGenerator;
    private final ExtractPluginRegistry extractPluginRegistry;
    private final LoadPluginRegistry loadPluginRegistry;

    public TaskExecutionContext createContext(String jobCode, GlobalConfig globalConfig,
                                              JobConfig jobConfig) {
        String batchNumber = batchNumberGenerator.nextBatchNumber();
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setJobCode(jobCode);
        entity.setBatchNumber(batchNumber);
        entity.setStatus(TaskExecutionStatus.RUNNING);
        TaskExecutionEntity saved = taskExecutionRepository.create(entity);
        return new TaskExecutionContext(saved.getTaskId(), batchNumber, jobCode, globalConfig, jobConfig);
    }

    public void executeExtract(TaskExecutionContext context) {
        Long taskId = context.getTaskId();
        try {
            taskLogger.logProgress(taskId, "INIT", 0, "初始化任务");
            Path workDir = initWorkDir(context, true);
            // 通过插件执行结构化数据导出/非结构化采集
            ExtractPlugin plugin = extractPluginRegistry.select(context);
            if (plugin != null) {
                plugin.extract(context);
                taskLogger.logProgress(taskId, "EXPORT", 50, "数据导出完成");
            } else {
                taskLogger.logProgress(taskId, "EXPORT", 10, "未找到匹配的卸载插件，跳过导出");
            }
            Path tarGz = compressAndSplit(context, workDir);
            taskLogger.logProgress(taskId, "POST_PROCESS", 80, "压缩与分片完成，主文件=" + tarGz);
            // TODO: 加密与 manifest.json 生成
            taskLogger.markSuccess(taskId, "卸载任务完成");
        } catch (Exception e) {
            log.error("executeExtract failed, taskId={}", taskId, e);
            taskLogger.markFailed(taskId, "卸载任务失败", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void executeLoad(TaskExecutionContext context) {
        Long taskId = context.getTaskId();
        try {
            taskLogger.logProgress(taskId, "INIT", 0, "初始化加载任务");
            Path workDir = initWorkDir(context, false);
            // 从 input_directory 解压到 workDir（解密、合并分片为 TODO，当前仅解压主包）
            unpackToWorkDir(context, workDir);
            taskLogger.logProgress(taskId, "UNPACK", 30, "解压完成，工作目录=" + workDir);
            LoadPlugin plugin = loadPluginRegistry.select(context);
            if (plugin != null) {
                plugin.load(context);
                taskLogger.logProgress(taskId, "LOAD", 80, "数据加载完成");
            } else {
                taskLogger.logProgress(taskId, "LOAD", 50, "未找到匹配的加载插件，跳过加载");
            }
            taskLogger.markSuccess(taskId, "加载任务完成");
        } catch (Exception e) {
            log.error("executeLoad failed, taskId={}", taskId, e);
            taskLogger.markFailed(taskId, "加载任务失败", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void unpackToWorkDir(TaskExecutionContext context, Path workDir) throws IOException {
        JobConfig jobConfig = context.getJobConfig();
        String inputDirStr = jobConfig.getInputDirectory();
        if (inputDirStr == null || inputDirStr.isEmpty()) {
            return;
        }
        Path inputDir = Paths.get(inputDirStr);
        if (Files.notExists(inputDir) || !Files.isDirectory(inputDir)) {
            return;
        }
        // 查找主 tar.gz（不含分片 .001/.002）
        try (java.util.stream.Stream<Path> stream = Files.list(inputDir)) {
            Path archive = stream
                .filter(Files::isRegularFile)
                .filter(p -> {
                    String name = p.getFileName().toString();
                    return name.endsWith(".tar.gz") && !name.matches(".*\\.\\d{3}$");
                })
                .findFirst()
                .orElse(null);
            if (archive != null) {
                compressionManager.decompressTarGz(archive, workDir);
            }
        }
    }

    private Path initWorkDir(TaskExecutionContext context, boolean extract) throws IOException {
        GlobalConfig globalConfig = context.getGlobalConfig();
        JobConfig jobConfig = context.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String baseWorkDir = jobConfig.getWorkDir();
        if (baseWorkDir == null && globalConfig.getExtract() != null) {
            baseWorkDir = globalConfig.getExtract().getWorkDir();
        }
        if (baseWorkDir == null) {
            baseWorkDir = "work";
        }
        Path root = Paths.get(baseWorkDir, jobName, context.getBatchNumber());
        fileSystemManager.ensureDirectory(root);
        return root;
    }

    private Path compressAndSplit(TaskExecutionContext context, Path workDir) throws IOException {
        GlobalConfig globalConfig = context.getGlobalConfig();
        JobConfig jobConfig = context.getJobConfig();
        String jobName = jobConfig.getJob().getName();
        String exchangeDir = jobConfig.getExchangeDir();
        if (exchangeDir == null && globalConfig.getExtract() != null) {
            exchangeDir = globalConfig.getExtract().getWorkDir();
        }
        if (exchangeDir == null) {
            exchangeDir = "exchange";
        }
        Path targetDir = Paths.get(exchangeDir, jobName, context.getBatchNumber());
        fileSystemManager.ensureDirectory(targetDir);
        Path tarGz = targetDir.resolve(jobName + "_" + context.getBatchNumber() + ".tar.gz");
        compressionManager.compressToTarGz(workDir, tarGz);
        GlobalConfig.CompressionConfig compression = globalConfig.getCompression();
        if (compression != null && compression.getSplitThresholdGb() != null) {
            long thresholdBytes = (long) (compression.getSplitThresholdGb() * 1024 * 1024 * 1024);
            compressionManager.split(tarGz, thresholdBytes);
        }
        return tarGz;
    }
}

