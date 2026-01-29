package org.csits.kel.manager.compression;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalCompressionManagerTest {

    private LocalCompressionManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        manager = new LocalCompressionManager();
    }

    @Test
    void compressToTarGz_and_decompressTarGz_roundTrip() throws IOException {
        Path sourceDir = tempDir.resolve("source");
        Files.createDirectories(sourceDir);
        Path f1 = sourceDir.resolve("f1.txt");
        Path sub = sourceDir.resolve("sub");
        Files.createDirectories(sub);
        Path f2 = sub.resolve("f2.txt");
        Files.write(f1, "content1".getBytes());
        Files.write(f2, "content2".getBytes());

        Path archive = tempDir.resolve("out.tar.gz");
        manager.compressToTarGz(sourceDir, archive);
        assertThat(Files.exists(archive)).isTrue();

        Path targetDir = tempDir.resolve("decompressed");
        manager.decompressTarGz(archive, targetDir);

        assertThat(Files.readAllBytes(targetDir.resolve("f1.txt"))).isEqualTo("content1".getBytes());
        assertThat(Files.readAllBytes(targetDir.resolve("sub").resolve("f2.txt"))).isEqualTo("content2".getBytes());
    }

    @Test
    void split_whenFileLargerThanThreshold_createsMultipleParts() throws IOException {
        Path largeFile = tempDir.resolve("large.bin");
        byte[] data = new byte[3 * 1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(largeFile, data);

        long thresholdBytes = 1024 * 1024;
        List<Path> parts = manager.split(largeFile, thresholdBytes);

        assertThat(parts).hasSizeGreaterThan(1);
        long totalBytes = 0;
        for (Path p : parts) {
            totalBytes += Files.size(p);
        }
        assertThat(totalBytes).isEqualTo(Files.size(largeFile));
    }

    @Test
    void split_whenFileSmallerThanOrEqualToThreshold_returnsSingleFile() throws IOException {
        Path smallFile = tempDir.resolve("small.txt");
        Files.write(smallFile, "small".getBytes());

        List<Path> result = manager.split(smallFile, 1024);

        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(smallFile);
    }
}
