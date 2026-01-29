package org.csits.kel.manager.compression;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * 打包压缩与分片管理。
 */
public interface CompressionManager {

    /**
     * 将目录打成 tar.gz 包。
     */
    Path compressToTarGz(Path sourceDir, Path targetFile) throws IOException;

    /**
     * 按阈值分片。
     */
    List<Path> split(Path sourceFile, long thresholdBytes) throws IOException;

    /**
     * 将 tar.gz 解压到目标目录。
     */
    void decompressTarGz(Path archive, Path targetDir) throws IOException;

    /**
     * 合并分片文件并解压到目标目录。
     *
     * @param inputDir 输入目录（包含分片文件或主压缩包）
     * @param outputDir 输出目录
     * @return 输出目录
     */
    Path mergeAndDecompress(Path inputDir, Path outputDir) throws IOException;
}

