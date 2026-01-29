package org.csits.kel.server.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 文件交付服务
 * 实现安全的文件交付流程：先写入.tmp文件，完成后重命名为最终文件
 */
@Slf4j
@Service
public class FileDeliveryService {

    private static final String TMP_SUFFIX = ".tmp";

    /**
     * 开始文件交付，返回临时文件路径
     *
     * @param targetPath 目标文件路径
     * @return 临时文件路径
     */
    public Path beginDelivery(Path targetPath) throws IOException {
        Path tmpPath = getTempPath(targetPath);

        // 确保父目录存在
        Path parentDir = tmpPath.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        // 如果临时文件已存在，删除它（可能是上次失败的残留）
        if (Files.exists(tmpPath)) {
            log.warn("临时文件已存在，删除: {}", tmpPath);
            Files.delete(tmpPath);
        }

        log.debug("开始文件交付: target={}, tmp={}", targetPath, tmpPath);
        return tmpPath;
    }

    /**
     * 完成文件交付，将临时文件重命名为最终文件
     *
     * @param targetPath 目标文件路径
     * @return 最终文件路径
     */
    public Path completeDelivery(Path targetPath) throws IOException {
        Path tmpPath = getTempPath(targetPath);

        if (!Files.exists(tmpPath)) {
            throw new IOException("临时文件不存在: " + tmpPath);
        }

        // 如果目标文件已存在，先删除（或备份）
        if (Files.exists(targetPath)) {
            log.warn("目标文件已存在，将被覆盖: {}", targetPath);
            Files.delete(targetPath);
        }

        // 重命名临时文件为最终文件
        Files.move(tmpPath, targetPath, StandardCopyOption.ATOMIC_MOVE);
        log.info("文件交付完成: {}", targetPath);

        return targetPath;
    }

    /**
     * 取消文件交付，删除临时文件
     *
     * @param targetPath 目标文件路径
     */
    public void cancelDelivery(Path targetPath) {
        Path tmpPath = getTempPath(targetPath);

        try {
            if (Files.exists(tmpPath)) {
                Files.delete(tmpPath);
                log.info("已取消文件交付，删除临时文件: {}", tmpPath);
            }
        } catch (IOException e) {
            log.error("删除临时文件失败: {}", tmpPath, e);
        }
    }

    /**
     * 复制文件到目标位置（使用.tmp临时文件）
     *
     * @param source 源文件
     * @param target 目标文件
     * @return 最终文件路径
     */
    public Path deliverFile(Path source, Path target) throws IOException {
        if (!Files.exists(source)) {
            throw new IOException("源文件不存在: " + source);
        }

        // 开始交付
        Path tmpPath = beginDelivery(target);

        try {
            // 复制文件到临时位置
            Files.copy(source, tmpPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("文件已复制到临时位置: {} -> {}", source, tmpPath);

            // 完成交付
            return completeDelivery(target);

        } catch (Exception e) {
            // 失败时取消交付
            cancelDelivery(target);
            throw new IOException("文件交付失败: " + source + " -> " + target, e);
        }
    }

    /**
     * 移动文件到目标位置（使用.tmp临时文件）
     *
     * @param source 源文件
     * @param target 目标文件
     * @return 最终文件路径
     */
    public Path deliverFileByMove(Path source, Path target) throws IOException {
        if (!Files.exists(source)) {
            throw new IOException("源文件不存在: " + source);
        }

        // 开始交付
        Path tmpPath = beginDelivery(target);

        try {
            // 移动文件到临时位置
            Files.move(source, tmpPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("文件已移动到临时位置: {} -> {}", source, tmpPath);

            // 完成交付
            return completeDelivery(target);

        } catch (Exception e) {
            // 失败时取消交付
            cancelDelivery(target);
            throw new IOException("文件交付失败: " + source + " -> " + target, e);
        }
    }

    /**
     * 批量交付文件
     *
     * @param sourceDir 源目录
     * @param targetDir 目标目录
     * @param pattern 文件匹配模式（如 "*.tar.gz"）
     * @return 交付的文件数量
     */
    public int deliverFiles(Path sourceDir, Path targetDir, String pattern) throws IOException {
        if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
            throw new IOException("源目录不存在或不是目录: " + sourceDir);
        }

        // 确保目标目录存在
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        int count = 0;
        try (java.util.stream.Stream<Path> stream = Files.list(sourceDir)) {
            java.util.List<Path> files = stream
                .filter(Files::isRegularFile)
                .filter(p -> matchesPattern(p.getFileName().toString(), pattern))
                .collect(java.util.stream.Collectors.toList());

            for (Path file : files) {
                Path target = targetDir.resolve(file.getFileName());
                deliverFile(file, target);
                count++;
            }
        }

        log.info("批量交付完成: {} 个文件从 {} 到 {}", count, sourceDir, targetDir);
        return count;
    }

    /**
     * 获取临时文件路径
     */
    private Path getTempPath(Path targetPath) {
        String fileName = targetPath.getFileName().toString();
        Path parent = targetPath.getParent();
        return parent != null
            ? parent.resolve(fileName + TMP_SUFFIX)
            : Path.of(fileName + TMP_SUFFIX);
    }

    /**
     * 简单的文件名模式匹配
     */
    private boolean matchesPattern(String fileName, String pattern) {
        if (pattern == null || pattern.equals("*")) {
            return true;
        }

        // 简单的通配符匹配
        if (pattern.startsWith("*")) {
            return fileName.endsWith(pattern.substring(1));
        } else if (pattern.endsWith("*")) {
            return fileName.startsWith(pattern.substring(0, pattern.length() - 1));
        } else {
            return fileName.equals(pattern);
        }
    }

    /**
     * 清理目录中的所有.tmp文件
     *
     * @param directory 目录路径
     * @return 清理的文件数量
     */
    public int cleanupTempFiles(Path directory) {
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return 0;
        }

        int count = 0;
        try (java.util.stream.Stream<Path> stream = Files.list(directory)) {
            java.util.List<Path> tmpFiles = stream
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(TMP_SUFFIX))
                .collect(java.util.stream.Collectors.toList());

            for (Path tmpFile : tmpFiles) {
                try {
                    Files.delete(tmpFile);
                    count++;
                    log.debug("清理临时文件: {}", tmpFile);
                } catch (IOException e) {
                    log.warn("清理临时文件失败: {}", tmpFile, e);
                }
            }
        } catch (IOException e) {
            log.error("清理临时文件失败: {}", directory, e);
        }

        if (count > 0) {
            log.info("清理了 {} 个临时文件: {}", count, directory);
        }
        return count;
    }
}
