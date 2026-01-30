package org.csits.kel.manager.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.springframework.stereotype.Component;

/**
 * 本地 tar.gz 压缩及简单分片实现。
 */
@Slf4j
@Component
public class LocalCompressionManager implements CompressionManager {

    @Override
    public Path compressToTarGz(Path sourceDir, Path targetFile) throws IOException {
        Files.createDirectories(targetFile.getParent());
        try (FileOutputStream fos = new FileOutputStream(targetFile.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

            Files.walk(sourceDir)
                .filter(path -> !Files.isDirectory(path))
                .forEach(path -> {
                    String entryName = sourceDir.relativize(path).toString();
                    TarArchiveEntry entry = new TarArchiveEntry(path.toFile(), entryName);
                    try {
                        taos.putArchiveEntry(entry);
                        Files.copy(path, taos);
                        taos.closeArchiveEntry();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            taos.finish();
        }
        return targetFile;
    }

    @Override
    public List<Path> split(Path sourceFile, long thresholdBytes) throws IOException {
        List<Path> result = new ArrayList<>();
        if (Files.size(sourceFile) <= thresholdBytes) {
            result.add(sourceFile);
            return result;
        }
        int index = 1;
        byte[] buffer = new byte[1024 * 1024];
        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(sourceFile.toFile()))) {
            int read;
            long writtenInPart = 0L;
            Path currentPart = null;
            BufferedOutputStream out = null;
            while ((read = in.read(buffer)) != -1) {
                if (out == null || writtenInPart >= thresholdBytes) {
                    if (out != null) {
                        out.flush();
                        out.close();
                    }
                    String partName = sourceFile.toString() + String.format(".%03d", index++);
                    currentPart = sourceFile.getParent().resolve(Paths.get(partName).getFileName());
                    out = new BufferedOutputStream(new FileOutputStream(currentPart.toFile()));
                    result.add(currentPart);
                    writtenInPart = 0L;
                }
                out.write(buffer, 0, read);
                writtenInPart += read;
            }
            if (out != null) {
                out.flush();
                out.close();
            }
        }
        return result;
    }

    @Override
    public void decompressTarGz(Path archive, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (FileInputStream fis = new FileInputStream(archive.toFile());
             BufferedInputStream bis = new BufferedInputStream(fis);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(bis);
             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextTarEntry()) != null) {
                Path dest = targetDir.resolve(entry.getName()).normalize();
                if (!dest.startsWith(targetDir)) {
                    throw new IOException("Entry path escapes target dir: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(dest);
                } else {
                    Files.createDirectories(dest.getParent());
                    Files.copy(tais, dest, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    @Override
    public Path mergeAndDecompress(Path inputDir, Path outputDir) throws IOException {
        if (!Files.exists(inputDir) || !Files.isDirectory(inputDir)) {
            throw new IOException("输入目录不存在: " + inputDir);
        }

        // 1. 查找分片文件（.tar.gz.001, .002, ...）
        List<Path> splits = findSplitFiles(inputDir);

        if (splits.isEmpty()) {
            // 无分片，查找主文件
            Path mainArchive = findMainArchive(inputDir);
            if (mainArchive != null) {
                log.info("未找到分片文件，直接解压主文件: {}", mainArchive);
                decompressTarGz(mainArchive, outputDir);
                return outputDir;
            } else {
                throw new IOException("未找到压缩包或分片文件: " + inputDir);
            }
        }

        // 2. 合并分片到临时文件
        log.info("找到 {} 个分片文件，开始合并", splits.size());
        Path merged = Files.createTempFile("kel-merged-", ".tar.gz");
        try {
            try (FileOutputStream out = new FileOutputStream(merged.toFile())) {
                for (Path split : splits) {
                    log.debug("合并分片: {}", split.getFileName());
                    Files.copy(split, out);
                }
            }

            // 3. 解压合并后的文件
            log.info("分片合并完成，开始解压");
            decompressTarGz(merged, outputDir);

            return outputDir;
        } finally {
            // 4. 清理临时文件
            Files.deleteIfExists(merged);
        }
    }

    /**
     * 查找分片文件
     */
    private List<Path> findSplitFiles(Path dir) throws IOException {
        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(p -> {
                    String name = p.getFileName().toString();
                    // 匹配 *.tar.gz.001, *.tar.gz.002 等
                    return name.contains(".tar.gz.") && name.matches(".*\\.tar\\.gz\\.\\d{3}$");
                })
                .sorted((p1, p2) -> {
                    // 按文件名排序，确保 .001, .002, .003 的顺序
                    return p1.getFileName().toString().compareTo(p2.getFileName().toString());
                })
                .collect(java.util.stream.Collectors.toList());
        }
    }

    /**
     * 查找主压缩包（不含分片后缀）
     */
    private Path findMainArchive(Path dir) throws IOException {
        try (java.util.stream.Stream<Path> stream = Files.list(dir)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(p -> {
                    String name = p.getFileName().toString();
                    return name.endsWith(".tar.gz") && !name.matches(".*\\.\\d{3}$");
                })
                .findFirst()
                .orElse(null);
        }
    }
}

