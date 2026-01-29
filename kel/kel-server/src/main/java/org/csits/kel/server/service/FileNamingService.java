package org.csits.kel.server.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.FileNamingConfig;
import org.springframework.stereotype.Service;

/**
 * 文件命名服务
 * 实现标准文件命名规范：[系统]_[接口标识]_[版本号]_[日期]_[序号]_[Q/Z].TXT
 */
@Slf4j
@Service
public class FileNamingService {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 生成标准文件名
     *
     * @param config 文件命名配置
     * @param tableName 表名
     * @param sequenceNumber 序号
     * @param isIncremental 是否增量（true=增量Q，false=全量Z）
     * @return 标准文件名
     */
    public String generateStandardFileName(FileNamingConfig config, String tableName,
                                           int sequenceNumber, boolean isIncremental) {
        if (config == null || !config.isEnableStandardNaming()) {
            // 未启用标准命名，使用简单命名
            return tableName.replace(".", "_") + ".txt";
        }

        String systemCode = config.getSystemCode() != null ? config.getSystemCode() : "SYS";
        String interfaceCode = config.getInterfaceCode(tableName);
        String version = config.getVersion() != null ? config.getVersion() : "V01";
        String date = LocalDateTime.now().format(DATE_FORMATTER);
        String sequence = String.format("%03d", sequenceNumber);
        String type = isIncremental ? "Q" : "Z";

        String fileName = String.format("%s_%s_%s_%s_%s_%s.TXT",
            systemCode, interfaceCode, version, date, sequence, type);

        log.debug("生成标准文件名: table={}, fileName={}", tableName, fileName);
        return fileName;
    }

    /**
     * 生成SQL查询结果文件名
     *
     * @param config 文件命名配置
     * @param sqlName SQL名称
     * @param sequenceNumber 序号
     * @param isIncremental 是否增量
     * @return 标准文件名
     */
    public String generateSqlFileName(FileNamingConfig config, String sqlName,
                                      int sequenceNumber, boolean isIncremental) {
        if (config == null || !config.isEnableStandardNaming()) {
            // 未启用标准命名，使用简单命名
            return sqlName + ".txt";
        }

        String systemCode = config.getSystemCode() != null ? config.getSystemCode() : "SYS";
        String interfaceCode = config.getSqlInterfaceCode(sqlName);
        String version = config.getVersion() != null ? config.getVersion() : "V01";
        String date = LocalDateTime.now().format(DATE_FORMATTER);
        String sequence = String.format("%03d", sequenceNumber);
        String type = isIncremental ? "Q" : "Z";

        String fileName = String.format("%s_%s_%s_%s_%s_%s.TXT",
            systemCode, interfaceCode, version, date, sequence, type);

        log.debug("生成SQL文件名: sqlName={}, fileName={}", sqlName, fileName);
        return fileName;
    }

    /**
     * 生成简单文件名（兼容模式）
     */
    public String generateSimpleFileName(String name) {
        return name.replace(".", "_") + ".txt";
    }
}
