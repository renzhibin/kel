package org.csits.kel.manager.security;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 基于Bouncy Castle的国密算法实现。
 * 使用流式处理支持大文件加密解密，避免内存溢出。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "kel.crypto.provider", havingValue = "bouncycastle", matchIfMissing = true)
public class BouncyCastleSmCryptoManager implements SmCryptoManager {

    private static final int BUFFER_SIZE = 8192; // 8KB缓冲区
    private static final int SM4_KEY_SIZE = 16; // SM4密钥长度16字节
    private static final int SM4_IV_SIZE = 16; // SM4 IV长度16字节

    static {
        // 注册Bouncy Castle Provider
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
            log.info("Bouncy Castle Provider已注册");
        }
    }

    @Override
    public String calculateSm3(Path file) throws IOException {
        try {
            SM3Digest digest = new SM3Digest();
            byte[] buffer = new byte[BUFFER_SIZE];

            // 流式计算哈希，避免大文件OOM
            try (InputStream in = Files.newInputStream(file)) {
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, bytesRead);
                }
            }

            // 获取哈希值
            byte[] hash = new byte[digest.getDigestSize()];
            digest.doFinal(hash, 0);

            // 转换为十六进制字符串
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IOException("SM3计算失败: " + file, e);
        }
    }

    @Override
    public void encryptSm4(Path source, Path target, String key) throws IOException {
        try {
            // 准备密钥和IV
            byte[] keyBytes = prepareKey(key);
            byte[] iv = generateIV(key); // 基于密钥生成确定性IV

            // 初始化SM4加密器
            Cipher cipher = Cipher.getInstance("SM4/CBC/PKCS5Padding", BouncyCastleProvider.PROVIDER_NAME);
            SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "SM4");
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);

            // 流式加密
            try (InputStream in = Files.newInputStream(source);
                 OutputStream out = Files.newOutputStream(target)) {

                // 先写入IV（解密时需要）
                out.write(iv);

                // 使用CipherOutputStream流式加密
                try (CipherOutputStream cos = new CipherOutputStream(out, cipher)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) != -1) {
                        cos.write(buffer, 0, bytesRead);
                    }
                }
            }

            log.debug("SM4加密完成: {} -> {}", source.getFileName(), target.getFileName());
        } catch (Exception e) {
            throw new IOException("SM4加密失败: " + source, e);
        }
    }

    @Override
    public void decryptSm4(Path source, Path target, String key) throws IOException {
        try {
            byte[] keyBytes = prepareKey(key);

            try (InputStream in = Files.newInputStream(source);
                 OutputStream out = Files.newOutputStream(target)) {

                // 读取IV（前16字节）
                byte[] iv = new byte[SM4_IV_SIZE];
                int ivBytesRead = in.read(iv);
                if (ivBytesRead != SM4_IV_SIZE) {
                    throw new IOException("无法读取IV，文件可能已损坏");
                }

                // 初始化解密器
                Cipher cipher = Cipher.getInstance("SM4/CBC/PKCS5Padding", BouncyCastleProvider.PROVIDER_NAME);
                SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "SM4");
                IvParameterSpec ivSpec = new IvParameterSpec(iv);
                cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);

                // 流式解密
                try (CipherInputStream cis = new CipherInputStream(in, cipher)) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int bytesRead;
                    while ((bytesRead = cis.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                    }
                }
            }

            log.debug("SM4解密完成: {} -> {}", source.getFileName(), target.getFileName());
        } catch (Exception e) {
            throw new IOException("SM4解密失败: " + source, e);
        }
    }

    /**
     * 准备SM4密钥（16字节）
     */
    private byte[] prepareKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("SM4密钥不能为空");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[SM4_KEY_SIZE];

        if (keyBytes.length >= SM4_KEY_SIZE) {
            // 密钥过长，截取前16字节
            System.arraycopy(keyBytes, 0, result, 0, SM4_KEY_SIZE);
        } else {
            // 密钥过短，填充到16字节
            System.arraycopy(keyBytes, 0, result, 0, keyBytes.length);
            // 剩余部分用0填充
            for (int i = keyBytes.length; i < SM4_KEY_SIZE; i++) {
                result[i] = 0;
            }
        }

        return result;
    }

    /**
     * 生成IV（基于密钥生成确定性IV，确保相同密钥生成相同IV）
     */
    private byte[] generateIV(String key) {
        // 使用密钥的SM3哈希前16字节作为IV
        SM3Digest digest = new SM3Digest();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        digest.update(keyBytes, 0, keyBytes.length);

        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);

        byte[] iv = new byte[SM4_IV_SIZE];
        System.arraycopy(hash, 0, iv, 0, SM4_IV_SIZE);
        return iv;
    }
}
