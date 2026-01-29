package org.csits.kel.manager.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SimpleSmCryptoManagerTest {

    private SimpleSmCryptoManager manager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        manager = new SimpleSmCryptoManager();
    }

    @Test
    void calculateSm3_sameContentSameDigest() throws IOException {
        Path file = tempDir.resolve("same.txt");
        Files.write(file, "same content".getBytes());
        String digest1 = manager.calculateSm3(file);
        String digest2 = manager.calculateSm3(file);
        assertThat(digest1).isEqualTo(digest2);
        assertThat(digest1).matches("[0-9a-f]{64}");
    }

    @Test
    void calculateSm3_differentContentDifferentDigest() throws IOException {
        Path f1 = tempDir.resolve("a.txt");
        Path f2 = tempDir.resolve("b.txt");
        Files.write(f1, "content A".getBytes());
        Files.write(f2, "content B".getBytes());
        assertThat(manager.calculateSm3(f1)).isNotEqualTo(manager.calculateSm3(f2));
    }

    @Test
    void encryptSm4_placeholderCopiesFile() throws IOException {
        Path source = tempDir.resolve("plain.txt");
        Files.write(source, "secret".getBytes());
        Path target = tempDir.resolve("encrypted.bin");

        manager.encryptSm4(source, target, "key");

        assertThat(Files.exists(target)).isTrue();
        assertThat(Files.readAllBytes(target)).isEqualTo(Files.readAllBytes(source));
    }

    @Test
    void decryptSm4_placeholderCopiesFile() throws IOException {
        Path source = tempDir.resolve("cipher.bin");
        Files.write(source, "decrypted content".getBytes());
        Path target = tempDir.resolve("decrypted.txt");

        manager.decryptSm4(source, target, "key");

        assertThat(Files.exists(target)).isTrue();
        assertThat(Files.readAllBytes(target)).isEqualTo(Files.readAllBytes(source));
    }
}
