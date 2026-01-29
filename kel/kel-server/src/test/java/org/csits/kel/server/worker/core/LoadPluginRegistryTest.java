package org.csits.kel.server.worker.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.csits.kel.manager.plugin.LoadPlugin;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LoadPluginRegistryTest {

    private TaskExecutionContext context;
    private LoadPlugin supportingPlugin;
    private LoadPlugin nonSupportingPlugin;

    @BeforeEach
    void setUp() {
        context = new TaskExecutionContext();
        supportingPlugin = mock(LoadPlugin.class);
        when(supportingPlugin.supports(any())).thenReturn(true);
        nonSupportingPlugin = mock(LoadPlugin.class);
        when(nonSupportingPlugin.supports(any())).thenReturn(false);
    }

    @Test
    void select_returnsFirstSupportingPlugin() {
        LoadPluginRegistry registry = new LoadPluginRegistry(
            Arrays.asList(nonSupportingPlugin, supportingPlugin));
        LoadPlugin selected = registry.select(context);
        assertThat(selected).isSameAs(supportingPlugin);
    }

    @Test
    void select_returnsNullWhenNoneSupport() {
        LoadPluginRegistry registry = new LoadPluginRegistry(
            Collections.singletonList(nonSupportingPlugin));
        LoadPlugin selected = registry.select(context);
        assertThat(selected).isNull();
    }

    @Test
    void select_returnsNullWhenPluginsEmpty() {
        LoadPluginRegistry registry = new LoadPluginRegistry(Collections.emptyList());
        LoadPlugin selected = registry.select(context);
        assertThat(selected).isNull();
    }
}
