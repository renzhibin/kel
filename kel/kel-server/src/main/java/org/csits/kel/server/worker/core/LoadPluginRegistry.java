package org.csits.kel.server.worker.core;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.csits.kel.manager.plugin.LoadPlugin;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.springframework.stereotype.Component;

/**
 * 加载插件注册与选择器。
 */
@Component
@RequiredArgsConstructor
public class LoadPluginRegistry {

    private final List<LoadPlugin> plugins;

    public LoadPlugin select(TaskExecutionContext context) {
        return plugins.stream()
            .filter(p -> p.supports(context))
            .findFirst()
            .orElse(null);
    }
}

