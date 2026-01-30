package org.csits.kel.dao;

import java.time.LocalDateTime;
import lombok.Data;

/**
 * 配置表实体，对应 kel.job_config。
 */
@Data
public class JobConfigEntity {

    private Long id;
    private String configKey;
    private String contentYaml;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
