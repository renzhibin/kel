package org.csits.kel.manager.security;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BouncyCastleSmCryptoManager单元测试
 */
class BouncyCastleSmCryptoManagerTest {

    private BouncyCastleSmCryptoManager cryptoManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        cryptoManager = new BouncyCastleSmCryptoManager();
    }

    @Test
    void testCalculateSm3_EmptyFile() throws IOException {
        // 创建空文件
        Path file = tempDir.resolve("empty.txt");
        Files.createFile(file);

        // 计算SM3
        String hash = cryptoManager.calculateSm3(file);

        // 验证哈希值不为空且长度正确（SM3输出256位=64个十六进制字符）
        assertNotNull(hash);
        assertEquals(64, hash.length());
        // SM3空文件的哈希值（已知测试向量）
        assertEquals("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b", hash);
    }

    @Test
    void testCalculateSm3_WithContent() throws IOException {
        // 创建包含内容的文件
        Path file = tempDir.resolve("test.txt");
        String content = "Hello, SM3!";
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));

        // 计算SM3
        String hash = cryptoManager.calculateSm3(file);

        // 验证哈希值
        assertNotNull(hash);
        assertEquals(64, hash.length());
        // 相同内容应产生相同哈希
        String hash2 = cryptoManager.calculateSm3(file);
        assertEquals(hash, hash2);
    }

    @Test
    void testCalculateSm3_LargeFile() throws IOException {
        // 创建大文件（10MB）
        Path file = tempDir.resolve("large.txt");
        byte[] data = new byte[10 * 1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(file, data);

        // 计算SM3（测试流式处理）
        String hash = cryptoManager.calculateSm3(file);

        assertNotNull(hash);
        assertEquals(64, hash.length());
    }

    @Test
    void testEncryptDecryptSm4_SmallFile() throws IOException {
        // 创建测试文件
        Path source = tempDir.resolve("source.txt");
        String content = "This is a test for SM4 encryption!";
        Files.write(source, content.getBytes(StandardCharsets.UTF_8));

        // 加密
        Path encrypted = tempDir.resolve("encrypted.bin");
        String key = "test_key_123456";
        cryptoManager.encryptSm4(source, encrypted, key);

        // 验证加密文件存在且大小不同
        assertTrue(Files.exists(encrypted));
        assertTrue(Files.size(encrypted) > 0);

        // 解密
        Path decrypted = tempDir.resolve("decrypted.txt");
        cryptoManager.decryptSm4(encrypted, decrypted, key);

        // 验证解密后内容一致
        String decryptedContent = new String(Files.readAllBytes(decrypted), StandardCharsets.UTF_8);
        assertEquals(content, decryptedContent);
    }

    @Test
    void testEncryptDecryptSm4_LargeFile() throws IOException {
        // 创建大文件（5MB）
        Path source = tempDir.resolve("large_source.bin");
        byte[] data = new byte[5 * 1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        Files.write(source, data);

        // 加密
        Path encrypted = tempDir.resolve("large_encrypted.bin");
        String key = "large_file_key_123";
        cryptoManager.encryptSm4(source, encrypted, key);

        assertTrue(Files.exists(encrypted));

        // 解密
        Path decrypted = tempDir.resolve("large_decrypted.bin");
        cryptoManager.decryptSm4(encrypted, decrypted, key);

        // 验证内容一致
        byte[] decryptedData = Files.readAllBytes(decrypted);
        assertArrayEquals(data, decryptedData);
    }

    @Test
    void testEncryptDecryptSm4_WrongKey() throws IOException {
        // 创建测试文件
        Path source = tempDir.resolve("source.txt");
        Files.write(source, "Secret data".getBytes(StandardCharsets.UTF_8));

        // 使用密钥1加密
        Path encrypted = tempDir.resolve("encrypted.bin");
        cryptoManager.encryptSm4(source, encrypted, "key1");

        // 使用密钥2解密（应该失败或得到乱码）
        Path decrypted = tempDir.resolve("decrypted.txt");
        assertThrows(IOException.class, () -> {
            cryptoManager.decryptSm4(encrypted, decrypted, "key2");
        });
    }

    @Test
    void testSm4KeyPreparation() throws IOException {
        // 测试不同长度的密钥
        Path source = tempDir.resolve("test.txt");
        Files.write(source, "Test".getBytes(StandardCharsets.UTF_8));

        // 短密钥
        Path enc1 = tempDir.resolve("enc1.bin");
        cryptoManager.encryptSm4(source, enc1, "short");

        // 长密钥
        Path enc2 = tempDir.resolve("enc2.bin");
        cryptoManager.encryptSm4(source, enc2, "this_is_a_very_long_key_that_exceeds_16_bytes");

        // 正好16字节密钥
        Path enc3 = tempDir.resolve("enc3.bin");
        cryptoManager.encryptSm4(source, enc3, "exactly16bytes!!");

        // 所有加密都应该成功
        assertTrue(Files.exists(enc1));
        assertTrue(Files.exists(enc2));
        assertTrue(Files.exists(enc3));
    }

    @Test
    void testSm4DeterministicIV() throws IOException {
        // 测试相同密钥生成相同IV
        Path source = tempDir.resolve("test.txt");
        Files.write(source, "Test content".getBytes(StandardCharsets.UTF_8));

        String key = "same_key";

        // 第一次加密
        Path enc1 = tempDir.resolve("enc1.bin");
        cryptoManager.encryptSm4(source, enc1, key);

        // 第二次加密
        Path enc2 = tempDir.resolve("enc2.bin");
        cryptoManager.encryptSm4(source, enc2, key);

        // 相同密钥和内容应产生相同的加密结果
        byte[] data1 = Files.readAllBytes(enc1);
        byte[] data2 = Files.readAllBytes(enc2);
        assertArrayEquals(data1, data2);
    }

    @Test
    void testEmptyKey() throws IOException {
        Path source = tempDir.resolve("test.txt");
        Files.write(source, "test".getBytes(StandardCharsets.UTF_8));

        // 空密钥应该抛出异常（被包装在IOException中）
        IOException ex1 = assertThrows(IOException.class, () -> {
            cryptoManager.encryptSm4(source, tempDir.resolve("enc.bin"), "");
        });
        assertTrue(ex1.getCause() instanceof IllegalArgumentException);

        IOException ex2 = assertThrows(IOException.class, () -> {
            cryptoManager.encryptSm4(source, tempDir.resolve("enc.bin"), null);
        });
        assertTrue(ex2.getCause() instanceof IllegalArgumentException);
    }
}
