package org.csits.kel.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
import org.csits.kel.server.dto.ManifestMetadata;
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
    private final ManifestService manifestService;
    private final RetryService retryService;
    private final MetricsCollector metricsCollector;
    private final DiskSpaceChecker diskSpaceChecker;
    private final FileDeliveryService fileDeliveryService;
    private final TaskStateMachine taskStateMachine;
    private final ProgressTracker progressTracker;

    public TaskExecutionContext createContext(String jobCode, GlobalConfig globalConfig,
                                              JobConfig jobConfig) {
        String batchNumber = batchNumberGenerator.nextBatchNumber();
        TaskExecutionEntity entity = new TaskExecutionEntity();
        entity.setJobCode(jobCode);
        entity.setBatchNumber(batchNumber);
        entity.setStatus(TaskExecutionStatus.RUNNING.name());
        entity.setProgress(0);
        entity.setCurrentStage("INIT");

        // 序列化配置快照
        try {
            ObjectMapper mapper = new ObjectMapper();
            String configSnapshot = mapper.writeValueAsString(jobConfig);
            entity.setConfigSnapshot(configSnapshot);
        } catch (Exception e) {
            log.warn("序列化配置快照失败", e);
        }

        TaskExecutionEntity saved = taskExecutionRepository.save(entity);
        return new TaskExecutionContext(saved.getTaskId(), batchNumber, jobCode, globalConfig, jobConfig);
    }

    public void executeExtract(TaskExecutionContext context) {
        Long taskId = context.getTaskId();
        try {
            // 使用重试机制执行卸载任务
            retryService.executeWithRetryVoid(() -> {
                executeExtractInternal(context);
            }, context.getGlobalConfig().getRetry(), "卸载任务");
        } catch (Exception e) {
            log.error("executeExtract failed after retries, taskId={}", taskId, e);
            taskLogger.markFailed(taskId, "卸载任务失败", e.getMessage());
            taskStateMachine.markFailed(taskId, "卸载任务失败", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void executeExtractInternal(TaskExecutionContext context) throws Exception {
        Long taskId = context.getTaskId();

        // 标记任务为运行中
        taskStateMachine.markRunning(taskId, "开始执行卸载任务");

        // 检查磁盘空间
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.DISK_CHECK, 0);
        GlobalConfig globalConfig = context.getGlobalConfig();
        JobConfig jobConfig = context.getJobConfig();
        String workDir = jobConfig.getWorkDir();
        if (workDir == null && globalConfig.getExtract() != null) {
            workDir = globalConfig.getExtract().getWorkDir();
        }
        if (workDir == null) {
            workDir = "work";
        }

        String exchangeDir = jobConfig.getExchangeDir();
        if (exchangeDir == null && globalConfig.getExtract() != null) {
            exchangeDir = globalConfig.getExtract().getWorkDir();
        }
        if (exchangeDir == null) {
            exchangeDir = "exchange";
        }

        // 检查工作目录和交换目录的磁盘空间
        if (!diskSpaceChecker.checkMultiplePaths(new String[]{workDir, exchangeDir}, globalConfig)) {
            throw new RuntimeException("磁盘空间不足，无法执行卸载任务");
        }
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.DISK_CHECK, 100);

        // 初始化指标收集
        metricsCollector.initTaskStatistics(taskId, context.getBatchNumber());

        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.INIT, 0);
        taskLogger.logProgress(taskId, "INIT", 0, "初始化任务");
        metricsCollector.recordStageStart(taskId, "INIT");
        Path workDirPath = initWorkDir(context, true);
        metricsCollector.recordStageEnd(taskId, "INIT", "SUCCESS", "初始化完成");
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.INIT, 100);

        // 通过插件执行结构化数据导出/非结构化采集
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.EXPORT, 0);
        metricsCollector.recordStageStart(taskId, "EXPORT");
        ExtractPlugin plugin = extractPluginRegistry.select(context);
        if (plugin != null) {
            plugin.extract(context);
            taskLogger.logProgress(taskId, "EXPORT", 50, "数据导出完成");
            metricsCollector.recordStageEnd(taskId, "EXPORT", "SUCCESS", "数据导出完成");
        } else {
            taskLogger.logProgress(taskId, "EXPORT", 10, "未找到匹配的卸载插件，跳过导出");
            metricsCollector.recordStageEnd(taskId, "EXPORT", "SKIPPED", "未找到匹配的卸载插件");
        }
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.EXPORT, 100);

        // 生成manifest.json
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.MANIFEST, 0);
        metricsCollector.recordStageStart(taskId, "MANIFEST");
        ManifestMetadata manifest = manifestService.generateManifest(context, workDirPath);
        Path manifestFile = workDirPath.resolve("manifest.json");
        manifestService.writeManifest(manifest, manifestFile);
        taskLogger.logProgress(taskId, "MANIFEST", 60, "生成manifest.json完成");
        metricsCollector.recordStageEnd(taskId, "MANIFEST", "SUCCESS", "生成manifest完成");
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.MANIFEST, 100);

        // 压缩与分片
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.COMPRESS, 0);
        metricsCollector.recordStageStart(taskId, "COMPRESS");
        long originalSize = calculateDirectorySize(workDirPath);
        Path tarGz = compressAndSplit(context, workDirPath);
        long compressedSize = java.nio.file.Files.size(tarGz);
        metricsCollector.recordCompressionStats(taskId, originalSize, compressedSize);
        taskLogger.logProgress(taskId, "POST_PROCESS", 80, "压缩与分片完成，主文件=" + tarGz);
        metricsCollector.recordStageEnd(taskId, "COMPRESS", "SUCCESS", "压缩完成");
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.COMPRESS, 100);

        // 加密处理
        GlobalConfig globalConfigForEncrypt = context.getGlobalConfig();
        if (globalConfigForEncrypt.getSecurity() != null && Boolean.TRUE.equals(globalConfigForEncrypt.getSecurity().getEnableEncryption())) {
            String key = globalConfigForEncrypt.getSecurity().getSm4Key();
            if (key == null || key.isEmpty()) {
                log.warn("启用了加密但未配置SM4密钥，跳过加密");
            } else {
                progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.ENCRYPT, 0);
                metricsCollector.recordStageStart(taskId, "ENCRYPT");
                long encryptStart = System.currentTimeMillis();
                encryptFiles(tarGz.getParent(), key);
                long encryptDuration = System.currentTimeMillis() - encryptStart;
                metricsCollector.recordEncryptionStats(taskId, encryptDuration);
                taskLogger.logProgress(taskId, "ENCRYPT", 90, "文件加密完成");
                metricsCollector.recordStageEnd(taskId, "ENCRYPT", "SUCCESS", "加密完成");
                progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.ENCRYPT, 100);
            }
        }

        // 完成任务
        progressTracker.updateExtractProgress(taskId, ProgressTracker.ExtractStage.FINALIZE, 100);
        // 持久化统计信息
        metricsCollector.persistTaskStatistics(taskId);
        taskLogger.markSuccess(taskId, "卸载任务完成");
        taskStateMachine.markSuccess(taskId, "卸载任务完成");
    }

    public void executeLoad(TaskExecutionContext context) {
        Long taskId = context.getTaskId();
        try {
            // 使用重试机制执行加载任务
            retryService.executeWithRetryVoid(() -> {
                executeLoadInternal(context);
            }, context.getGlobalConfig().getRetry(), "加载任务");
        } catch (Exception e) {
            log.error("executeLoad failed after retries, taskId={}", taskId, e);
            taskLogger.markFailed(taskId, "加载任务失败", e.getMessage());
            taskStateMachine.markFailed(taskId, "加载任务失败", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void executeLoadInternal(TaskExecutionContext context) throws Exception {
        Long taskId = context.getTaskId();

        // 标记任务为运行中
        taskStateMachine.markRunning(taskId, "开始执行加载任务");

        // 检查磁盘空间
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.DISK_CHECK, 0);
        GlobalConfig globalConfig = context.getGlobalConfig();
        JobConfig jobConfig = context.getJobConfig();
        String workDir = jobConfig.getWorkDir();
        if (workDir == null && globalConfig.getExtract() != null) {
            workDir = globalConfig.getExtract().getWorkDir();
        }
        if (workDir == null) {
            workDir = "work";
        }

        // 检查工作目录的磁盘空间
        if (!diskSpaceChecker.checkDiskSpace(workDir, globalConfig)) {
            throw new RuntimeException("磁盘空间不足，无法执行加载任务");
        }
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.DISK_CHECK, 100);

        // 初始化指标收集
        metricsCollector.initTaskStatistics(taskId, context.getBatchNumber());

        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.INIT, 0);
        taskLogger.logProgress(taskId, "INIT", 0, "初始化加载任务");
        metricsCollector.recordStageStart(taskId, "INIT");
        Path workDirPath = initWorkDir(context, false);
        metricsCollector.recordStageEnd(taskId, "INIT", "SUCCESS", "初始化完成");
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.INIT, 100);

        // 如果启用加密，先解密
        String inputDirStr = context.getJobConfig().getInputDirectory();
        if (inputDirStr != null && !inputDirStr.isEmpty()) {
            Path inputDir = Paths.get(inputDirStr);
            GlobalConfig globalConfigForDecrypt = context.getGlobalConfig();
            if (globalConfigForDecrypt.getSecurity() != null && Boolean.TRUE.equals(globalConfigForDecrypt.getSecurity().getEnableEncryption())) {
                String key = globalConfigForDecrypt.getSecurity().getSm4Key();
                if (key != null && !key.isEmpty()) {
                    progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.DECRYPT, 0);
                    metricsCollector.recordStageStart(taskId, "DECRYPT");
                    long decryptStart = System.currentTimeMillis();
                    decryptFiles(inputDir, key);
                    long decryptDuration = System.currentTimeMillis() - decryptStart;
                    metricsCollector.recordEncryptionStats(taskId, decryptDuration);
                    taskLogger.logProgress(taskId, "DECRYPT", 10, "文件解密完成");
                    metricsCollector.recordStageEnd(taskId, "DECRYPT", "SUCCESS", "解密完成");
                    progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.DECRYPT, 100);
                }
            }
        }

        // 从 input_directory 解压到 workDir（自动处理分片合并）
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.UNPACK, 0);
        metricsCollector.recordStageStart(taskId, "UNPACK");
        unpackToWorkDir(context, workDirPath);
        taskLogger.logProgress(taskId, "UNPACK", 30, "解压完成，工作目录=" + workDirPath);
        metricsCollector.recordStageEnd(taskId, "UNPACK", "SUCCESS", "解压完成");
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.UNPACK, 100);

        // 解析并校验manifest
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.VALIDATE, 0);
        metricsCollector.recordStageStart(taskId, "VALIDATE");
        Path manifestFile = workDirPath.resolve("manifest.json");
        if (Files.exists(manifestFile)) {
            ManifestMetadata manifest = manifestService.parseManifest(manifestFile);
            boolean valid = manifestService.validateManifest(manifest, workDirPath);
            if (!valid) {
                throw new RuntimeException("Manifest校验失败，数据文件可能损坏");
            }
            taskLogger.logProgress(taskId, "VALIDATE", 40, "Manifest校验通过");
            metricsCollector.recordStageEnd(taskId, "VALIDATE", "SUCCESS", "校验通过");
        } else {
            metricsCollector.recordStageEnd(taskId, "VALIDATE", "SKIPPED", "无manifest文件");
        }
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.VALIDATE, 100);

        // 加载数据
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.LOAD, 0);
        metricsCollector.recordStageStart(taskId, "LOAD");
        LoadPlugin plugin = loadPluginRegistry.select(context);
        if (plugin != null) {
            plugin.load(context);
            taskLogger.logProgress(taskId, "LOAD", 80, "数据加载完成");
            metricsCollector.recordStageEnd(taskId, "LOAD", "SUCCESS", "数据加载完成");
        } else {
            taskLogger.logProgress(taskId, "LOAD", 50, "未找到匹配的加载插件，跳过加载");
            metricsCollector.recordStageEnd(taskId, "LOAD", "SKIPPED", "未找到匹配的加载插件");
        }
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.LOAD, 100);

        // 完成任务
        progressTracker.updateLoadProgress(taskId, ProgressTracker.LoadStage.FINALIZE, 100);
        // 持久化统计信息
        metricsCollector.persistTaskStatistics(taskId);
        taskLogger.markSuccess(taskId, "加载任务完成");
        taskStateMachine.markSuccess(taskId, "加载任务完成");
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

        // 使用新的合并解压方法（自动处理分片）
        compressionManager.mergeAndDecompress(inputDir, workDir);
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

        // 使用临时文件名进行压缩
        String fileName = jobName + "_" + context.getBatchNumber() + ".tar.gz";
        Path finalTarGz = targetDir.resolve(fileName);
        Path tmpTarGz = fileDeliveryService.beginDelivery(finalTarGz);

        try {
            // 压缩到临时文件
            compressionManager.compressToTarGz(workDir, tmpTarGz);

            // 分片处理（如果需要）
            GlobalConfig.CompressionConfig compression = globalConfig.getCompression();
            if (compression != null && compression.getSplitThresholdGb() != null) {
                long thresholdBytes = (long) (compression.getSplitThresholdGb() * 1024 * 1024 * 1024);
                long fileSize = Files.size(tmpTarGz);

                if (fileSize > thresholdBytes) {
                    // 需要分片，先完成主文件交付
                    fileDeliveryService.completeDelivery(finalTarGz);
                    // 然后进行分片（分片会删除原文件并创建分片文件）
                    compressionManager.split(finalTarGz, thresholdBytes);
                    return finalTarGz;
                }
            }

            // 不需要分片，直接完成交付
            fileDeliveryService.completeDelivery(finalTarGz);
            return finalTarGz;

        } catch (Exception e) {
            // 失败时取消交付
            fileDeliveryService.cancelDelivery(finalTarGz);
            throw new IOException("压缩和交付失败", e);
        }
    }

    /**
     * 加密目录中的所有tar.gz文件（包括分片）
     */
    private void encryptFiles(Path dir, String key) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return;
        }

        List<Path> filesToEncrypt = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".tar.gz") ||
                            p.getFileName().toString().matches(".*\\.tar\\.gz\\.\\d{3}$"))
                .forEach(filesToEncrypt::add);
        }

        for (Path file : filesToEncrypt) {
            // 使用临时文件进行加密
            Path tmpEncrypted = fileDeliveryService.beginDelivery(file);
            try {
                smCryptoManager.encryptSm4(file, tmpEncrypted, key);
                // 删除原文件
                Files.delete(file);
                // 完成交付（重命名临时文件为最终文件）
                fileDeliveryService.completeDelivery(file);
                log.info("文件已加密: {}", file.getFileName());
            } catch (Exception e) {
                fileDeliveryService.cancelDelivery(file);
                throw new IOException("加密文件失败: " + file, e);
            }
        }
    }

    /**
     * 解密目录中的所有加密文件
     */
    private void decryptFiles(Path dir, String key) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return;
        }

        List<Path> filesToDecrypt = new ArrayList<>();
        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                .filter(p -> {
                    String name = p.getFileName().toString();
                    return name.endsWith(".tar.gz") || name.matches(".*\\.tar\\.gz\\.\\d{3}$");
                })
                .forEach(filesToDecrypt::add);
        }

        for (Path file : filesToDecrypt) {
            Path decrypted = file.getParent().resolve(file.getFileName() + ".dec");
            smCryptoManager.decryptSm4(file, decrypted, key);
            // 删除加密文件，重命名解密文件
            Files.delete(file);
            Files.move(decrypted, file);
            log.info("文件已解密: {}", file.getFileName());
        }
    }

    /**
     * 计算目录大小
     */
    private long calculateDirectorySize(Path dir) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return 0;
        }

        long size = 0;
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            size = stream
                .filter(Files::isRegularFile)
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .sum();
        }
        return size;
    }
}

