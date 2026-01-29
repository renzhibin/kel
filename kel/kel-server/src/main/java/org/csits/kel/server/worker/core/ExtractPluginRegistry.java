package org.csits.kel.server.worker.core;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Component;

/**
 * 简单的卸载插件注册与选择器。
 */
@Component
@RequiredArgsConstructor
public class ExtractPluginRegistry {

    private final List<ExtractPlugin> plugins;

    public ExtractPlugin select(TaskExecutionContext context) {
        return plugins.stream()
            .filter(p -> p.supports(context))
            .findFirst()
            .orElse(null);
    }
}

