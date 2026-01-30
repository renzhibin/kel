package org.csits.kel.server.service;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.GlobalConfig;
import org.springframework.stereotype.Service;

/**
 * 磁盘空间检查服务
 * 在任务执行前检查磁盘可用空间，防止因空间不足导致任务失败
 */
@Slf4j
@Service
public class DiskSpaceChecker {

    /**
     * 检查指定路径的磁盘空间是否充足
     *
     * @param path 要检查的路径
     * @param config 全局配置
     * @return 是否有足够空间
     */
    public boolean checkDiskSpace(String path, GlobalConfig config) {
        if (config.getDiskProtection() == null || !Boolean.TRUE.equals(config.getDiskProtection().getEnabled())) {
            log.debug("磁盘水位保护未启用，跳过检查");
            return true;
        }

        try {
            // 规范化路径，处理 .. 和 . 符号，并转换为绝对路径
            Path targetPath = Paths.get(path).toAbsolutePath().normalize();
            if (!Files.exists(targetPath)) {
                // 如果路径不存在，检查父目录
                targetPath = targetPath.getParent();
                if (targetPath == null) {
                    targetPath = Paths.get(".").toAbsolutePath().normalize();
                }
            }

            FileStore fileStore = Files.getFileStore(targetPath);
            long usableSpace = fileStore.getUsableSpace();
            long totalSpace = fileStore.getTotalSpace();

            // 计算可用空间百分比
            double usablePercentage = (double) usableSpace / totalSpace * 100;

            // 获取配置的最小可用空间要求
            Double minFreeSpaceGb = config.getDiskProtection().getMinFreeSpaceGb();
            Double minFreeSpacePercent = config.getDiskProtection().getMinFreeSpacePercent();

            // 转换为GB
            double usableSpaceGb = usableSpace / (1024.0 * 1024.0 * 1024.0);

            log.info("磁盘空间检查: path={}, usable={}GB ({}%), total={}GB",
                path, String.format("%.2f", usableSpaceGb),
                String.format("%.2f", usablePercentage),
                String.format("%.2f", totalSpace / (1024.0 * 1024.0 * 1024.0)));

            // 检查绝对空间要求
            if (minFreeSpaceGb != null && usableSpaceGb < minFreeSpaceGb) {
                log.error("磁盘可用空间不足: 需要至少{}GB，当前仅有{}GB",
                    minFreeSpaceGb, String.format("%.2f", usableSpaceGb));
                return false;
            }

            // 检查百分比要求
            if (minFreeSpacePercent != null && usablePercentage < minFreeSpacePercent) {
                log.error("磁盘可用空间百分比不足: 需要至少{}%，当前仅有{}%",
                    minFreeSpacePercent, String.format("%.2f", usablePercentage));
                return false;
            }

            log.info("磁盘空间检查通过");
            return true;

        } catch (IOException e) {
            log.error("检查磁盘空间失败: path={}", path, e);
            // 检查失败时，根据配置决定是否允许继续
            if (config.getDiskProtection().getFailOnCheckError() != null
                && config.getDiskProtection().getFailOnCheckError()) {
                return false;
            }
            return true;
        }
    }

    /**
     * 检查多个路径的磁盘空间
     *
     * @param paths 要检查的路径列表
     * @param config 全局配置
     * @return 是否所有路径都有足够空间
     */
    public boolean checkMultiplePaths(String[] paths, GlobalConfig config) {
        for (String path : paths) {
            if (!checkDiskSpace(path, config)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取磁盘空间信息
     *
     * @param path 路径
     * @return 磁盘空间信息的Map
     */
    public java.util.Map<String, Object> getDiskSpaceInfo(String path) {
        java.util.Map<String, Object> info = new java.util.HashMap<>();
        try {
            // 规范化路径，处理 .. 和 . 符号，并转换为绝对路径
            Path targetPath = Paths.get(path).toAbsolutePath().normalize();
            if (!Files.exists(targetPath)) {
                targetPath = targetPath.getParent();
                if (targetPath == null) {
                    targetPath = Paths.get(".").toAbsolutePath().normalize();
                }
            }

            FileStore fileStore = Files.getFileStore(targetPath);
            long usableSpace = fileStore.getUsableSpace();
            long totalSpace = fileStore.getTotalSpace();
            long usedSpace = totalSpace - usableSpace;

            info.put("path", path);
            info.put("totalSpaceGb", totalSpace / (1024.0 * 1024.0 * 1024.0));
            info.put("usableSpaceGb", usableSpace / (1024.0 * 1024.0 * 1024.0));
            info.put("usedSpaceGb", usedSpace / (1024.0 * 1024.0 * 1024.0));
            info.put("usablePercentage", (double) usableSpace / totalSpace * 100);
            info.put("usedPercentage", (double) usedSpace / totalSpace * 100);

        } catch (IOException e) {
            log.error("获取磁盘空间信息失败: path={}", path, e);
            info.put("error", e.getMessage());
        }
        return info;
    }
}
