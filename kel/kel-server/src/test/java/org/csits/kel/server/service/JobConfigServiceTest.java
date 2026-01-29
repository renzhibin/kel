package org.csits.kel.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.core.io.DefaultResourceLoader;

class JobConfigServiceTest {

    private JobConfigService service;
    private YamlConfigLoader yamlConfigLoader;
    private DefaultResourceLoader resourceLoader;

    @BeforeEach
    void setUp() {
        resourceLoader = new DefaultResourceLoader();
        yamlConfigLoader = new YamlConfigLoader();
        service = new JobConfigService(yamlConfigLoader, resourceLoader);
        ReflectionTestUtils.setField(service, "confBaseDir", "classpath:conf");
    }

    @Test
    void loadMergedConfig_jobRuntimeTableConcurrencyOverridesGlobal() throws IOException {
        JobConfigService.MergedResult result = service.loadMergedConfig("merge_job", true);

        assertThat(result).isNotNull();
        assertThat(result.getGlobalConfig()).isNotNull();
        assertThat(result.getJobConfig()).isNotNull();
        assertThat(result.getJobConfig().getRuntime().getTableConcurrency()).isEqualTo(10);
        assertThat(result.getGlobalConfig().getConcurrency().getDefaultTableConcurrency()).isEqualTo(10);
    }

    @Test
    void loadGlobalConfig_loadsFromClasspath() throws IOException {
        GlobalConfig config = service.loadGlobalConfig();
        assertThat(config).isNotNull();
        assertThat(config.getConcurrency().getDefaultTableConcurrency()).isEqualTo(3);
    }

    @Test
    void loadJobConfig_loadsFromClasspath() throws IOException {
        JobConfig config = service.loadJobConfig("merge_job", true);
        assertThat(config).isNotNull();
        assertThat(config.getJob().getName()).isEqualTo("merge_job");
        assertThat(config.getRuntime().getTableConcurrency()).isEqualTo(10);
    }
}
