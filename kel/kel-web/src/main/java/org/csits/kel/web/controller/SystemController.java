package org.csits.kel.web.controller;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.service.DiskSpaceChecker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * 系统监控API接口
 */
@Slf4j
@RestController
@RequestMapping("/api/system")
@RequiredArgsConstructor
public class SystemController {

    private final DiskSpaceChecker diskSpaceChecker;

    /**
     * 获取磁盘空间信息
     */
    @GetMapping("/disk-space")
    public ResponseEntity<Map<String, Object>> getDiskSpace(@RequestParam String path) {
        Map<String, Object> info = diskSpaceChecker.getDiskSpaceInfo(path);
        return ResponseEntity.ok(info);
    }
}
