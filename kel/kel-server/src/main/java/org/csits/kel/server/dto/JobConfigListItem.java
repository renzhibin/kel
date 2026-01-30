package org.csits.kel.server.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 作业配置列表项，用于作业配置页列表展示。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobConfigListItem {

    private String configKey;
    private LocalDateTime updatedAt;
}
