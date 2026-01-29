package org.csits.kel.server.dto;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/**
 * 文件命名配置
 */
@Data
public class FileNamingConfig {

    /**
     * 系统标识
     */
    private String systemCode;

    /**
     * 版本号
     */
    private String version;

    /**
     * 表名到接口编码的映射
     * 例如: {"users": "J0001", "orders": "J0002"}
     */
    private Map<String, String> tableInterfaceMapping = new HashMap<>();

    /**
     * SQL名称到接口编码的映射
     * 例如: {"recent_orders": "J0003"}
     */
    private Map<String, String> sqlInterfaceMapping = new HashMap<>();

    /**
     * 是否启用标准命名
     */
    private boolean enableStandardNaming = false;

    /**
     * 获取表的接口编码
     */
    public String getInterfaceCode(String tableName) {
        return tableInterfaceMapping.getOrDefault(tableName, "J9999");
    }

    /**
     * 获取SQL的接口编码
     */
    public String getSqlInterfaceCode(String sqlName) {
        return sqlInterfaceMapping.getOrDefault(sqlName, "J9999");
    }
}
