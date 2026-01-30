package org.csits.kel.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.manager.security.SmCryptoManager;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.ManifestMetadata;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.plugin.kingbase.KingbaseExtractPlugin;
import org.springframework.stereotype.Service;

/**
 * Manifest服务，负责生成、解析和校验manifest.json
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ManifestService {

    private final SmCryptoManager smCryptoManager;
    private final ObjectMapper objectMapper;

    /**
     * 生成manifest.json
     *
     * @param context 任务执行上下文
     * @param workDir 工作目录
     * @return Manifest元数据
     */
    public ManifestMetadata generateManifest(TaskExecutionContext context, Path workDir) throws IOException {
        ManifestMetadata manifest = new ManifestMetadata();

        // 基本信息
        manifest.setJobName(context.getJobName());
        manifest.setBatchNumber(context.getBatchNumber());
        manifest.setTimestamp(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        // 压缩配置
        GlobalConfig.CompressionConfig compression = context.getGlobalConfig().getCompression();
        if (compression != null) {
            ManifestMetadata.CompressionInfo compressionInfo = new ManifestMetadata.CompressionInfo();
            compressionInfo.setAlgorithm(compression.getAlgorithm());
            compressionInfo.setSplitThresholdGb(compression.getSplitThresholdGb());
            manifest.setCompression(compressionInfo);
        }

        // 加密配置
        GlobalConfig.SecurityConfig security = context.getGlobalConfig().getSecurity();
        if (security != null) {
            ManifestMetadata.EncryptionInfo encryptionInfo = new ManifestMetadata.EncryptionInfo();
            encryptionInfo.setEnabled(security.getEnableEncryption());
            encryptionInfo.setAlgorithm("SM4");
            manifest.setEncryption(encryptionInfo);
        }

        // 数据文件清单
        List<ManifestMetadata.FileInfo> fileInfos = new ArrayList<>();
        Path dataDir = workDir.resolve("data");
        if (Files.exists(dataDir) && Files.isDirectory(dataDir)) {
            // 从context中获取导出结果
            List<KingbaseExtractPlugin.TableExportResult> exportResults = context.getAttribute("exportResults");
            if (exportResults != null) {
                for (KingbaseExtractPlugin.TableExportResult result : exportResults) {
                    ManifestMetadata.FileInfo fileInfo = new ManifestMetadata.FileInfo();
                    Path file = result.getFilePath();
                    fileInfo.setName("data/" + file.getFileName().toString());
                    fileInfo.setSize(Files.size(file));
                    fileInfo.setSm3(smCryptoManager.calculateSm3(file));
                    fileInfo.setTableName(result.getTableName());
                    fileInfo.setRowCount(result.getRowCount());
                    fileInfos.add(fileInfo);
                }
            }
        }
        manifest.setFiles(fileInfos);

        log.info("生成manifest，包含 {} 个数据文件", fileInfos.size());
        return manifest;
    }

    /**
     * 添加分片信息到manifest
     *
     * @param manifest Manifest元数据
     * @param splitFiles 分片文件列表
     */
    public void addSplitInfo(ManifestMetadata manifest, List<Path> splitFiles) throws IOException {
        List<ManifestMetadata.SplitInfo> splitInfos = new ArrayList<>();
        int index = 1;
        for (Path splitFile : splitFiles) {
            ManifestMetadata.SplitInfo splitInfo = new ManifestMetadata.SplitInfo();
            splitInfo.setName(splitFile.getFileName().toString());
            splitInfo.setSize(Files.size(splitFile));
            splitInfo.setSm3(smCryptoManager.calculateSm3(splitFile));
            splitInfo.setIndex(index++);
            splitInfos.add(splitInfo);
        }
        manifest.setSplits(splitInfos);
        log.info("添加 {} 个分片文件到manifest", splitInfos.size());
    }

    /**
     * 写入manifest.json到文件
     *
     * @param manifest Manifest元数据
     * @param manifestFile manifest文件路径
     */
    public void writeManifest(ManifestMetadata manifest, Path manifestFile) throws IOException {
        ObjectMapper mapper = objectMapper.copy();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(manifestFile.toFile(), manifest);
        log.info("Manifest已写入: {}", manifestFile);
    }

    /**
     * 解析manifest.json
     *
     * @param manifestFile manifest文件路径
     * @return Manifest元数据
     */
    public ManifestMetadata parseManifest(Path manifestFile) throws IOException {
        if (!Files.exists(manifestFile)) {
            throw new IOException("Manifest文件不存在: " + manifestFile);
        }
        ManifestMetadata manifest = objectMapper.readValue(manifestFile.toFile(), ManifestMetadata.class);
        log.info("解析manifest: jobName={}, batchNumber={}, files={}",
            manifest.getJobName(), manifest.getBatchNumber(),
            manifest.getFiles() != null ? manifest.getFiles().size() : 0);
        return manifest;
    }

    /**
     * 校验manifest完整性
     *
     * @param manifest Manifest元数据
     * @param dataDir 数据目录
     * @return 是否校验通过
     */
    public boolean validateManifest(ManifestMetadata manifest, Path dataDir) {
        if (manifest.getFiles() == null || manifest.getFiles().isEmpty()) {
            log.warn("Manifest中没有文件记录");
            return true; // 空manifest视为有效
        }

        boolean allValid = true;
        for (ManifestMetadata.FileInfo fileInfo : manifest.getFiles()) {
            Path file = dataDir.resolve(fileInfo.getName());
            if (!Files.exists(file)) {
                log.error("文件不存在: {}", fileInfo.getName());
                allValid = false;
                continue;
            }

            try {
                long actualSize = Files.size(file);
                if (actualSize != fileInfo.getSize()) {
                    log.error("文件大小不匹配: {} (期望={}, 实际={})",
                        fileInfo.getName(), fileInfo.getSize(), actualSize);
                    allValid = false;
                    continue;
                }

                String actualSm3 = smCryptoManager.calculateSm3(file);
                if (!actualSm3.equals(fileInfo.getSm3())) {
                    log.error("SM3校验和不匹配: {} (期望={}, 实际={})",
                        fileInfo.getName(), fileInfo.getSm3(), actualSm3);
                    allValid = false;
                    continue;
                }

                log.debug("文件校验通过: {}", fileInfo.getName());
            } catch (IOException e) {
                log.error("校验文件失败: {}", fileInfo.getName(), e);
                allValid = false;
            }
        }

        if (allValid) {
            log.info("Manifest校验通过，所有文件完整");
        } else {
            log.error("Manifest校验失败，存在文件损坏或缺失");
        }

        return allValid;
    }

    /**
     * 查找分片文件
     *
     * @param dir 目录
     * @param baseName 基础文件名（不含扩展名）
     * @return 分片文件列表（按序号排序）
     */
    public List<Path> findSplitFiles(Path dir, String baseName) throws IOException {
        if (!Files.exists(dir) || !Files.isDirectory(dir)) {
            return new ArrayList<>();
        }

        return Files.list(dir)
            .filter(Files::isRegularFile)
            .filter(p -> {
                String name = p.getFileName().toString();
                // 匹配 baseName.001, baseName.002 等
                return name.startsWith(baseName + ".") && name.matches(".*\\.\\d{3}$");
            })
            .sorted((p1, p2) -> {
                // 按文件名排序，确保 .001, .002, .003 的顺序
                return p1.getFileName().toString().compareTo(p2.getFileName().toString());
            })
            .collect(Collectors.toList());
    }
}
