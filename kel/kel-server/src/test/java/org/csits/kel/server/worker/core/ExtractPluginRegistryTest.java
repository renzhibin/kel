package org.csits.kel.server.worker.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.csits.kel.manager.plugin.ExtractPlugin;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExtractPluginRegistryTest {

    private TaskExecutionContext context;
    private ExtractPlugin supportingPlugin;
    private ExtractPlugin nonSupportingPlugin;

    @BeforeEach
    void setUp() {
        context = new TaskExecutionContext();
        supportingPlugin = mock(ExtractPlugin.class);
        when(supportingPlugin.supports(any())).thenReturn(true);
        nonSupportingPlugin = mock(ExtractPlugin.class);
        when(nonSupportingPlugin.supports(any())).thenReturn(false);
    }

    @Test
    void select_returnsFirstSupportingPlugin() {
        ExtractPluginRegistry registry = new ExtractPluginRegistry(
            Arrays.asList(nonSupportingPlugin, supportingPlugin));
        ExtractPlugin selected = registry.select(context);
        assertThat(selected).isSameAs(supportingPlugin);
    }

    @Test
    void select_returnsNullWhenNoneSupport() {
        ExtractPluginRegistry registry = new ExtractPluginRegistry(
            Collections.singletonList(nonSupportingPlugin));
        ExtractPlugin selected = registry.select(context);
        assertThat(selected).isNull();
    }

    @Test
    void select_returnsNullWhenPluginsEmpty() {
        ExtractPluginRegistry registry = new ExtractPluginRegistry(Collections.emptyList());
        ExtractPlugin selected = registry.select(context);
        assertThat(selected).isNull();
    }
}
