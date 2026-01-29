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
}

