package org.csits.kel.server.service;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

/**
 * 加载并合并全局配置与作业配置。
 */
@Service
@RequiredArgsConstructor
public class JobConfigService {

    private final YamlConfigLoader yamlConfigLoader;

    private final ResourceLoader resourceLoader;

    /**
     * 全局配置文件路径根目录，例如 conf/dev。
     */
    @Value("${kel.conf.base-dir:classpath:conf/dev}")
    private String confBaseDir;

    public GlobalConfig loadGlobalConfig() throws IOException {
        Resource resource = resourceLoader.getResource(confBaseDir + "/global.yaml");
        return yamlConfigLoader.loadGlobalConfig(resource);
    }

    public JobConfig loadJobConfig(String jobCode) throws IOException {
        Resource resource = resourceLoader.getResource(
            confBaseDir + "/jobs/" + jobCode + ".yaml"
        );
        return yamlConfigLoader.loadJobConfig(resource);
    }

    /**
     * 合并配置：作业配置字段覆盖全局配置中对应部分。
     */
    public MergedResult loadMergedConfig(String jobCode) throws IOException {
        GlobalConfig global = loadGlobalConfig();
        JobConfig job = loadJobConfig(jobCode);
        GlobalConfig mergedGlobal = new GlobalConfig();
        BeanUtils.copyProperties(global, mergedGlobal);
        if (job.getRuntime() != null && job.getRuntime().getTableConcurrency() != null) {
            if (mergedGlobal.getConcurrency() == null) {
                mergedGlobal.setConcurrency(new GlobalConfig.ConcurrencyConfig());
            }
            mergedGlobal.getConcurrency().setDefaultTableConcurrency(
                job.getRuntime().getTableConcurrency()
            );
        }
        return new MergedResult(mergedGlobal, job);
    }

    public static class MergedResult {

        private final GlobalConfig globalConfig;

        private final JobConfig jobConfig;

        public MergedResult(GlobalConfig globalConfig, JobConfig jobConfig) {
            this.globalConfig = globalConfig;
            this.jobConfig = jobConfig;
        }

        public GlobalConfig getGlobalConfig() {
            return globalConfig;
        }

        public JobConfig getJobConfig() {
            return jobConfig;
        }
    }
}

