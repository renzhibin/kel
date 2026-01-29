package org.csits.kel.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

class YamlConfigLoaderTest {

    private YamlConfigLoader loader;

    @BeforeEach
    void setUp() {
        loader = new YamlConfigLoader();
    }

    @Test
    void loadGlobalConfig_parsesKeyFields() throws IOException {
        Resource resource = new ClassPathResource("conf/test_global.yaml");
        GlobalConfig config = loader.loadGlobalConfig(resource);

        assertThat(config).isNotNull();
        assertThat(config.getConcurrency()).isNotNull();
        assertThat(config.getConcurrency().getDefaultTableConcurrency()).isEqualTo(2);
        assertThat(config.getRetry().getMaxRetries()).isEqualTo(1);
        assertThat(config.getCompression().getAlgorithm()).isEqualTo("gzip");
        assertThat(config.getCompression().getSplitThresholdGb()).isEqualTo(3.5);
        assertThat(config.getExtract()).isNotNull();
        assertThat(config.getExtract().getWorkDir()).isEqualTo("work");
    }

    @Test
    void loadJobConfig_parsesKeyFields() throws IOException {
        Resource resource = new ClassPathResource("conf/jobs/test_job_extract.yaml");
        JobConfig config = loader.loadJobConfig(resource);

        assertThat(config).isNotNull();
        assertThat(config.getJob()).isNotNull();
        assertThat(config.getJob().getName()).isEqualTo("test_job");
        assertThat(config.getJob().getType().name()).isEqualTo("EXTRACT_KINGBASE");
        assertThat(config.getExtractDatabase()).isNotNull();
        assertThat(config.getExtractDatabase().getHost()).isEqualTo("127.0.0.1");
        assertThat(config.getExtractDatabase().getPort()).isEqualTo(5432);
        assertThat(config.getRuntime()).isNotNull();
        assertThat(config.getRuntime().getTableConcurrency()).isEqualTo(5);
    }
}
