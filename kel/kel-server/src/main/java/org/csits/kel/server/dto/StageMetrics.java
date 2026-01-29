package org.csits.kel.server.dto;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 阶段执行指标
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StageMetrics {
    private String stageName;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long durationMs;
    private String status;
    private String message;
}
