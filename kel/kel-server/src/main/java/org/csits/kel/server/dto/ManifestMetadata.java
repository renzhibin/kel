package org.csits.kel.server.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

/**
 * Manifest元数据，记录卸载任务的文件清单、校验和等信息。
 */
@Data
public class ManifestMetadata {

    /**
     * Manifest版本
     */
    private String version = "1.0";

    /**
     * 作业名
     */
    @JsonProperty("job_name")
    private String jobName;

    /**
     * 批次号
     */
    @JsonProperty("batch_number")
    private String batchNumber;

    /**
     * 生成时间戳（ISO 8601格式）
     */
    private String timestamp;

    /**
     * 压缩配置
     */
    private CompressionInfo compression;

    /**
     * 加密配置
     */
    private EncryptionInfo encryption;

    /**
     * 数据文件清单
     */
    private List<FileInfo> files;

    /**
     * 分片文件清单
     */
    private List<SplitInfo> splits;

    /**
     * 压缩信息
     */
    @Data
    public static class CompressionInfo {
        /**
         * 压缩算法
         */
        private String algorithm;

        /**
         * 分片阈值（GB）
         */
        @JsonProperty("split_threshold_gb")
        private Double splitThresholdGb;
    }

    /**
     * 加密信息
     */
    @Data
    public static class EncryptionInfo {
        /**
         * 是否启用加密
         */
        private Boolean enabled;

        /**
         * 加密算法
         */
        private String algorithm;
    }

    /**
     * 文件信息
     */
    @Data
    public static class FileInfo {
        /**
         * 文件名（相对路径）
         */
        private String name;

        /**
         * 文件大小（字节）
         */
        private Long size;

        /**
         * SM3校验和
         */
        private String sm3;

        /**
         * 表名（如果是表导出）
         */
        @JsonProperty("table_name")
        private String tableName;

        /**
         * 行数
         */
        @JsonProperty("row_count")
        private Long rowCount;
    }

    /**
     * 分片信息
     */
    @Data
    public static class SplitInfo {
        /**
         * 分片文件名
         */
        private String name;

        /**
         * 分片大小（字节）
         */
        private Long size;

        /**
         * SM3校验和
         */
        private String sm3;

        /**
         * 分片序号（从1开始）
         */
        private Integer index;
    }
}
