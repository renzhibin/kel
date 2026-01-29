package org.csits.kel.server.dto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * 任务统计信息
 */
@Data
public class TaskStatistics {
    private Long taskId;
    private String batchNumber;

    // 表级统计
    private Map<String, Long> tableRowCounts = new HashMap<>();
    private int totalTables;
    private long totalRows;

    // 文件统计
    private long totalFileSize;
    private int totalFiles;

    // 压缩统计
    private long originalSize;
    private long compressedSize;
    private Double compressionRatio;

    // 分片统计
    private int splitCount;
    private List<String> splitFiles = new ArrayList<>();

    // 阶段耗时统计
    private List<StageMetrics> stageMetrics = new ArrayList<>();
    private Long totalDurationMs;

    // 加密统计
    private boolean encrypted;
    private Long encryptionDurationMs;

    /**
     * 计算压缩率
     */
    public void calculateCompressionRatio() {
        if (originalSize > 0 && compressedSize > 0) {
            this.compressionRatio = (double) compressedSize / originalSize;
        }
    }

    /**
     * 添加表统计
     */
    public void addTableStats(String tableName, long rowCount) {
        tableRowCounts.put(tableName, rowCount);
        totalTables++;
        totalRows += rowCount;
    }

    /**
     * 添加阶段指标
     */
    public void addStageMetrics(StageMetrics metrics) {
        stageMetrics.add(metrics);
    }
}
