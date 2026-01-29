package org.csits.kel.manager.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.springframework.stereotype.Component;

/**
 * 本地 tar.gz 压缩及简单分片实现。
 */
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
                    Files.copy(tais, dest);
                }
            }
        }
    }
}

