package org.csits.kel.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.csits.kel.dao.JobConfigEntity;
import org.csits.kel.dao.JobConfigRepository;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.FileCopyUtils;

class JobConfigServiceTest {

    private JobConfigService service;
    private YamlConfigLoader yamlConfigLoader;
    private DefaultResourceLoader resourceLoader;
    private ResourcePatternResolver resourcePatternResolver;
    private JobConfigRepository jobConfigRepository;

    @BeforeEach
    void setUp() throws Exception {
        resourceLoader = new DefaultResourceLoader();
        resourcePatternResolver = new PathMatchingResourcePatternResolver(resourceLoader);
        yamlConfigLoader = new YamlConfigLoader();
        jobConfigRepository = org.mockito.Mockito.mock(JobConfigRepository.class);

        // 从 classpath 读取测试 YAML，供 mock 返回
        String globalYaml = readClasspathResource("classpath:conf/global.yaml");
        String mergeJobYaml = readClasspathResource("classpath:conf/jobs/merge_job_extract.yaml");

        JobConfigEntity globalEntity = new JobConfigEntity();
        globalEntity.setConfigKey("__global__");
        globalEntity.setContentYaml(globalYaml);
        JobConfigEntity mergeJobEntity = new JobConfigEntity();
        mergeJobEntity.setConfigKey("merge_job_extract");
        mergeJobEntity.setContentYaml(mergeJobYaml);

        when(jobConfigRepository.findByConfigKey("__global__")).thenReturn(Optional.of(globalEntity));
        when(jobConfigRepository.findByConfigKey("merge_job_extract")).thenReturn(Optional.of(mergeJobEntity));
        when(jobConfigRepository.findByConfigKey(anyString())).thenReturn(Optional.empty());

        service = new JobConfigService(yamlConfigLoader, resourceLoader, resourcePatternResolver, jobConfigRepository);
    }

    private String readClasspathResource(String path) throws Exception {
        Resource r = resourceLoader.getResource(path);
        byte[] bytes = FileCopyUtils.copyToByteArray(r.getInputStream());
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    void loadMergedConfig_jobRuntimeTableConcurrencyOverridesGlobal() throws Exception {
        JobConfigService.MergedResult result = service.loadMergedConfig("merge_job");

        assertThat(result).isNotNull();
        assertThat(result.getGlobalConfig()).isNotNull();
        assertThat(result.getJobConfig()).isNotNull();
        assertThat(result.getJobConfig().getRuntime().getTableConcurrency()).isEqualTo(10);
        assertThat(result.getGlobalConfig().getConcurrency().getDefaultTableConcurrency()).isEqualTo(10);
    }

    @Test
    void loadGlobalConfig_loadsFromDb() throws Exception {
        GlobalConfig config = service.loadGlobalConfig();
        assertThat(config).isNotNull();
        assertThat(config.getConcurrency().getDefaultTableConcurrency()).isEqualTo(3);
    }

    @Test
    void loadJobConfig_loadsFromDb() throws Exception {
        JobConfig jobConfig = service.loadJobConfig("merge_job_extract");
        assertThat(jobConfig).isNotNull();
        assertThat(jobConfig.getJob().getName()).isEqualTo("merge_job");
        assertThat(jobConfig.getRuntime().getTableConcurrency()).isEqualTo(10);
    }
}
