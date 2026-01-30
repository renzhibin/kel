package org.csits.kel.dao;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * 人工表级导出记录实体，对应 kel.manual_export。
 */
@Data
public class ManualExportEntity {

    private Long id;
    private String jobCode;
    private String tableName;
    private String mode;
    private String status;
    private Long taskId;
    private LocalDateTime requestedAt;
    private String requestedBy;
}
