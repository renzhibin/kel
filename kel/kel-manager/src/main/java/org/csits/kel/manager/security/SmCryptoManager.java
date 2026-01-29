package org.csits.kel.manager.security;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 国密相关处理抽象，后续可接入真实 SM3/SM4 实现。
 */
public interface SmCryptoManager {

    /**
     * 计算文件的 SM3 摘要。
     */
    String calculateSm3(Path file) throws IOException;

    /**
     * 使用 SM4 加密文件，输出到 target。
     */
    void encryptSm4(Path source, Path target, String key) throws IOException;

    /**
     * 使用 SM4 解密文件，输出到 target。
     */
    void decryptSm4(Path source, Path target, String key) throws IOException;
}

