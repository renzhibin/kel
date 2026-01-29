package org.csits.kel.manager.security;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.io.FileUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 占位实现：使用 SHA-256 模拟 SM3，按字节复制模拟 SM4。
 * 实际环境中可替换为真实国密库。
 *
 * 当配置 kel.crypto.provider=simple 时使用此实现。
 */
@Component
@ConditionalOnProperty(name = "kel.crypto.provider", havingValue = "simple")
public class SimpleSmCryptoManager implements SmCryptoManager {

    @Override
    public String calculateSm3(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] data = Files.readAllBytes(file);
            byte[] hash = digest.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Digest algorithm not available", e);
        }
    }

    @Override
    public void encryptSm4(Path source, Path target, String key) throws IOException {
        // 占位实现：直接复制文件
        FileUtils.copyFile(source.toFile(), target.toFile());
    }

    @Override
    public void decryptSm4(Path source, Path target, String key) throws IOException {
        // 占位实现：直接复制文件
        FileUtils.copyFile(source.toFile(), target.toFile());
    }
}

