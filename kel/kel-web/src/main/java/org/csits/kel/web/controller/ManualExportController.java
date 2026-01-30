package org.csits.kel.web.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.ManualExportEntity;
import org.csits.kel.server.service.ManualExportService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 人工表级导出记录 API。
 */
@Slf4j
@RestController
@RequestMapping("/api/manual-exports")
@RequiredArgsConstructor
public class ManualExportController {

    private final ManualExportService manualExportService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> list(
            @RequestParam(required = false) String jobName,
            @RequestParam(required = false) String tableName,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        List<ManualExportEntity> data = manualExportService.listManualExports(jobName, tableName, page, size);
        long total = manualExportService.countManualExports();
        Map<String, Object> result = new HashMap<>();
        result.put("data", data);
        result.put("total", total);
        result.put("page", page);
        result.put("size", size);
        return ResponseEntity.ok(result);
    }
}
