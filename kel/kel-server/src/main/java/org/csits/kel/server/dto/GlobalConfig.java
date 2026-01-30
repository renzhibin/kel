package org.csits.kel.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * 全局配置，对应 global.yaml。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class GlobalConfig {

    private ConcurrencyConfig concurrency;

    private RetryConfig retry;

    private CompressionConfig compression;

    /**
     * 是否启用压缩。
     */
    @JsonProperty("enable_compression")
    private Boolean enableCompression;

    private SecurityConfig security;

    private ExtractGlobalConfig extract;

    @JsonProperty("file_naming")
    private FileNamingConfig fileNaming;

    @JsonProperty("disk_protection")
    private DiskProtectionConfig diskProtection;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConcurrencyConfig {

        /**
         * 作业级默认并发。
         */
        @JsonProperty("default_table_concurrency")
        private Integer defaultTableConcurrency;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RetryConfig {

        private Integer maxRetries;

        /**
         * 重试间隔（秒）。
         */
        private Integer retryIntervalSec;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CompressionConfig {

        private String algorithm;

        /**
         * 分片阈值（GB）。
         */
        @JsonProperty("split_threshold_gb")
        private Double splitThresholdGb;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SecurityConfig {

        /**
         * SM4 密钥。
         */
        @JsonProperty("sm4_key")
        private String sm4Key;

        /**
         * 是否启用加密。YAML 中为 enable_encryption。
         */
        @JsonProperty("enable_encryption")
        private Boolean enableEncryption;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExtractGlobalConfig {

        /**
         * 工作目录。
         */
        @JsonProperty("work_dir")
        private String workDir;

        /**
         * 批次号格式，例如 {yyyyMMddHHmmss}_{seq}。
         */
        @JsonProperty("batch_number_format")
        private String batchNumberFormat;

        private String encoding;

        /**
         * 数据库版本，例如 V8R6。
         */
        private String databaseVersion;

        /**
         * 是否在完成后清理工作目录。
         */
        private Boolean cleanupWorkDir;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DiskProtectionConfig {

        /**
         * 是否启用磁盘水位保护
         */
        private Boolean enabled;

        /**
         * 最小可用空间（GB）
         */
        @JsonProperty("min_free_space_gb")
        private Double minFreeSpaceGb;

        /**
         * 最小可用空间百分比
         */
        @JsonProperty("min_free_space_percent")
        private Double minFreeSpacePercent;

        /**
         * 检查失败时是否拒绝执行
         */
        @JsonProperty("fail_on_check_error")
        private Boolean failOnCheckError;
    }
}

