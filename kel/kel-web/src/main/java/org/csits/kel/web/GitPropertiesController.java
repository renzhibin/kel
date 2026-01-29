package org.csits.kel.web;

import java.util.HashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 简单健康检查与 Git 信息占位接口。
 */
@RestController
@RequestMapping("/api")
public class GitPropertiesController {

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> body = new HashMap<>();
        body.put("status", "UP");
        return ResponseEntity.ok(body);
    }

    @GetMapping("/git")
    public ResponseEntity<Map<String, Object>> git() {
        Map<String, Object> body = new HashMap<>();
        body.put("branch", "unknown");
        body.put("commitId", "unknown");
        return ResponseEntity.ok(body);
    }
}

