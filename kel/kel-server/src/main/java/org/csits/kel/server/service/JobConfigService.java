package org.csits.kel.server.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.util.FileCopyUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.dao.JobConfigEntity;
import org.csits.kel.dao.JobConfigRepository;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.csits.kel.server.dto.JobConfigListItem;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.stereotype.Service;

/**
 * 加载并合并全局配置与作业配置。配置仅从数据库读取；可通过 importFromFiles 从 classpath YAML 导入到 DB。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JobConfigService {

    private final YamlConfigLoader yamlConfigLoader;
    private final ResourceLoader resourceLoader;
    private final ResourcePatternResolver resourcePatternResolver;
    private final JobConfigRepository jobConfigRepository;

    @Value("${kel.conf.base-dir:classpath:conf/dev}")
    private String confBaseDir;

    public GlobalConfig loadGlobalConfig() throws IOException {
        return loadGlobalConfigFromDb();
    }

    public JobConfig loadJobConfig(String configKey) throws IOException {
        return loadJobConfigFromDb(configKey);
    }

    /**
     * 合并配置。configKey 为完整 key（如 demo_extract、bss_kingbase_extract），或 jobCode + 类型由 type 拼出。
     * @param jobCode 作业编码，如 demo 或 bss_kingbase_extract
     * @param type 若 jobCode 不含 _extract/_load 则用此拼 configKey：jobCode + "_" + type（extract/load）
     */
    public MergedResult loadMergedConfig(String jobCode, String type) throws IOException {
        String configKey = resolveConfigKey(jobCode, type);
        GlobalConfig global = loadGlobalConfig();
        JobConfig job = loadJobConfig(configKey);
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

    /** 兼容旧调用：按 extract 尝试，再按 jobCode 原样。 */
    public MergedResult loadMergedConfig(String jobCode) throws IOException {
        if (jobCode != null && (jobCode.endsWith("_extract") || jobCode.endsWith("_load"))) {
            return loadMergedConfig(jobCode, null);
        }
        return loadMergedConfig(jobCode, "extract");
    }

    public List<String> listJobConfigKeys() {
        return jobConfigRepository.findAll().stream()
            .map(JobConfigEntity::getConfigKey)
            .collect(Collectors.toList());
    }

    /** 作业配置列表，用于作业配置页。 */
    public List<JobConfigListItem> listJobConfigs() {
        return jobConfigRepository.findAll().stream()
            .map(e -> new JobConfigListItem(e.getConfigKey(), e.getUpdatedAt()))
            .collect(Collectors.toList());
    }

    /** 获取作业配置原始 YAML 字符串，供编辑框使用。 */
    public String getRawContentYaml(String configKey) throws IOException {
        return jobConfigRepository.findByConfigKey(configKey)
            .map(JobConfigEntity::getContentYaml)
            .orElseThrow(() -> new IOException("未找到配置: " + configKey));
    }

    /**
     * 保存作业配置（创建或更新）。保存前校验 YAML 格式。
     */
    public void saveJobConfig(String configKey, String contentYaml) throws IOException {
        if (configKey == null || configKey.trim().isEmpty()) {
            throw new IllegalArgumentException("configKey 不能为空");
        }
        configKey = configKey.trim();
        if ("__global__".equals(configKey)) {
            yamlConfigLoader.loadGlobalConfigFromString(contentYaml);
        } else {
            yamlConfigLoader.loadJobConfigFromString(contentYaml);
        }
        JobConfigEntity e = new JobConfigEntity();
        e.setConfigKey(configKey);
        e.setContentYaml(contentYaml);
        jobConfigRepository.save(e);
    }

    /** 删除作业配置。禁止删除 __global__。 */
    public void deleteJobConfig(String configKey) {
        if ("__global__".equals(configKey)) {
            throw new IllegalArgumentException("不能删除全局配置 __global__");
        }
        jobConfigRepository.deleteByConfigKey(configKey);
    }

    /** 从 classpath YAML 资源导入到 DB（用于首次初始化或批量导入）。 */
    public void importFromFiles() throws IOException {
        Resource globalRes = resourceLoader.getResource(confBaseDir + "/global.yaml");
        if (globalRes.exists()) {
            byte[] bytes = FileCopyUtils.copyToByteArray(globalRes.getInputStream());
            String globalYaml = new String(bytes, StandardCharsets.UTF_8);
            JobConfigEntity e = new JobConfigEntity();
            e.setConfigKey("__global__");
            e.setContentYaml(globalYaml);
            jobConfigRepository.save(e);
            log.info("已导入 __global__");
        }
        List<String> keys = listJobConfigKeysFromClasspath();
        for (String key : keys) {
            Resource r = resourceLoader.getResource(confBaseDir + "/jobs/" + key + ".yaml");
            if (r.exists()) {
                byte[] bytes = FileCopyUtils.copyToByteArray(r.getInputStream());
                String yaml = new String(bytes, StandardCharsets.UTF_8);
                JobConfigEntity e = new JobConfigEntity();
                e.setConfigKey(key);
                e.setContentYaml(yaml);
                jobConfigRepository.save(e);
                log.info("已导入 job_config: {}", key);
            }
        }
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

    private static String resolveConfigKey(String jobCode, String type) {
        if (jobCode == null) {
            return "";
        }
        if (type == null || type.isEmpty() || jobCode.endsWith("_extract") || jobCode.endsWith("_load")) {
            return jobCode;
        }
        return jobCode + "_" + type;
    }

    private GlobalConfig loadGlobalConfigFromDb() throws IOException {
        return jobConfigRepository.findByConfigKey("__global__")
            .map(e -> {
                try {
                    return yamlConfigLoader.loadGlobalConfigFromString(e.getContentYaml());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            })
            .orElseThrow(() -> new IOException("DB 中未找到 __global__ 配置"));
    }

    private JobConfig loadJobConfigFromDb(String configKey) throws IOException {
        return jobConfigRepository.findByConfigKey(configKey)
            .map(e -> {
                try {
                    return yamlConfigLoader.loadJobConfigFromString(e.getContentYaml());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            })
            .orElseThrow(() -> new IOException("DB 中未找到配置: " + configKey));
    }

    /** 仅用于 importFromFiles：扫描 classpath jobs/*.yaml 得到 key 列表。 */
    private List<String> listJobConfigKeysFromClasspath() {
        try {
            String pattern = confBaseDir + "/jobs/*.yaml";
            if (!pattern.startsWith("classpath")) {
                pattern = "classpath:" + (pattern.startsWith("/") ? pattern.substring(1) : pattern);
            }
            Resource[] resources = resourcePatternResolver.getResources(pattern);
            List<String> keys = new java.util.ArrayList<>();
            for (Resource r : resources) {
                String name = r.getFilename();
                if (name != null && name.endsWith(".yaml")) {
                    keys.add(name.substring(0, name.length() - 5));
                }
            }
            return keys;
        } catch (Exception e) {
            log.debug("listJobConfigKeysFromClasspath failed: {}", e.getMessage());
            return java.util.Collections.emptyList();
        }
    }
}
