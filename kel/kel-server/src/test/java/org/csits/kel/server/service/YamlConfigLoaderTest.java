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

    @Test
    void loadJobConfig_newFormat_settingsResourcesTasks() throws IOException {
        Resource resource = new ClassPathResource("conf/jobs/new_format_job.yaml");
        JobConfig config = loader.loadJobConfig(resource);

        assertThat(config).isNotNull();
        assertThat(config.getJob()).isNotNull();
        assertThat(config.getJob().getName()).isEqualTo("daily_sync");
        assertThat(config.getJob().getType().name()).isEqualTo("EXTRACT_KINGBASE");

        // 兼容 getter：从 settings/resources 取值
        assertThat(config.getRuntime()).isNotNull();
        assertThat(config.getRuntime().getTableConcurrency()).isEqualTo(4);
        assertThat(config.getWorkDir()).isEqualTo("../data/work");
        assertThat(config.getExtractDatabase()).isNotNull();
        assertThat(config.getExtractDatabase().getHost()).isEqualTo("localhost");
        assertThat(config.getExtractDatabase().getPort()).isEqualTo(54321);
        assertThat(config.getExtractDirectory()).isEqualTo("../data/extract");
        assertThat(config.getExchangeDir()).isEqualTo("../data/exchange");
        assertThat(config.getInputDirectory()).isEqualTo("../data/input");
        assertThat(config.getTargetDirectory()).isEqualTo("../data/target");

        // 兼容 getter：从 tasks 转为 getExtractTasks() / getLoadTasks()
        assertThat(config.getExtractTasks()).hasSize(2);
        assertThat(config.getExtractTasks().get(0).getType().name()).isEqualTo("FULL");
        assertThat(config.getExtractTasks().get(0).getTables()).containsExactly("t_bond_info", "t_bond_trade");
        assertThat(config.getExtractTasks().get(1).getType().name()).isEqualTo("INCREMENTAL");
        assertThat(config.getExtractTasks().get(1).getSqlList()).hasSize(1);
        assertThat(config.getExtractTasks().get(1).getSqlList().get(0).getName()).isEqualTo("company_info_inc");

        assertThat(config.getLoadTasks()).hasSize(1);
        assertThat(config.getLoadTasks().get(0).getType().name()).isEqualTo("TRUNCATE_LOAD");
        assertThat(config.getLoadTasks().get(0).getInterfaceMapping()).containsEntry("J0001", "t_bond_info");
        assertThat(config.getLoadTasks().get(0).getEnableTransaction()).isTrue();
    }
}
