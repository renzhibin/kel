package org.csits.kel.dao;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * 人工表级导出记录实体，对应 kel.manual_export。
 */
@Data
public class ManualExportEntity {

    private Long id;
    /** EXPORT=表级卸载, LOAD=表级加载 */
    private String type;
    private String jobCode;
    private String tableName;
    private String mode;
    /** 仅 LOAD 时使用：源批次号 */
    private String sourceBatch;
    private String status;
    private Long taskId;
    private LocalDateTime requestedAt;
    private String requestedBy;
}
