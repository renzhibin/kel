package org.csits.kel.server.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.csits.kel.server.constants.JobType;
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
     * 合并配置。jobName 即配置唯一标识（DB config_key = YAML 内 job.name），传 job.name 即可加载。
     */
    public MergedResult loadMergedConfig(String jobName) throws IOException {
        if (jobName == null || jobName.trim().isEmpty()) {
            throw new IllegalArgumentException("jobName 不能为空");
        }
        String key = jobName.trim();
        GlobalConfig global = loadGlobalConfig();
        JobConfig job = loadJobConfig(key);
        String globalDbVersion = global.getExtract() != null ? global.getExtract().getDatabaseVersion() : null;
        if (job.getExtractDatabase() != null && isBlank(job.getExtractDatabase().getDatabaseVersion()) && globalDbVersion != null) {
            job.getExtractDatabase().setDatabaseVersion(globalDbVersion);
        }
        if (job.getTargetDatabase() != null && isBlank(job.getTargetDatabase().getDatabaseVersion()) && globalDbVersion != null) {
            job.getTargetDatabase().setDatabaseVersion(globalDbVersion);
        }
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

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    public List<String> listJobConfigKeys() {
        return jobConfigRepository.findAll().stream()
            .map(JobConfigEntity::getConfigKey)
            .collect(Collectors.toList());
    }

    /**
     * 表级卸载用：仅返回 job.type 为 EXTRACT_KINGBASE 的配置 key（不含 file_extract）。
     */
    public List<String> listConfigKeysForTableExport() {
        return filterConfigKeysByJobType(JobType.EXTRACT_KINGBASE);
    }

    /**
     * 表级加载用：仅返回 job.type 为 KINGBASE_LOAD 的配置 key（不含 file_load）。
     */
    public List<String> listConfigKeysForTableLoad() {
        return filterConfigKeysByJobType(JobType.KINGBASE_LOAD);
    }

    private List<String> filterConfigKeysByJobType(JobType expectedType) {
        List<String> all = listJobConfigKeys().stream()
            .filter(k -> k != null && !"__global__".equals(k))
            .collect(Collectors.toList());
        List<String> result = new ArrayList<>();
        for (String key : all) {
            try {
                JobConfig config = loadJobConfig(key);
                if (config.getJob() != null && expectedType == config.getJob().getType()) {
                    result.add(key);
                }
            } catch (Exception e) {
                log.debug("跳过配置 key={}: {}", key, e.getMessage());
            }
        }
        return result;
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
     * 保存作业配置（创建或更新）。存储 key 使用 YAML 内 job.name，保证 config_key 与 job.name 一致。
     * @param currentKey 当前 key（更新时传入，用于定位要更新的行）；新增时传 null，key 从 YAML 解析
     * @param contentYaml 配置 YAML
     */
    public void saveJobConfig(String currentKey, String contentYaml) throws IOException {
        if (contentYaml == null || contentYaml.trim().isEmpty()) {
            throw new IllegalArgumentException("contentYaml 不能为空");
        }
        if (currentKey != null && "__global__".equals(currentKey.trim())) {
            yamlConfigLoader.loadGlobalConfigFromString(contentYaml);
            JobConfigEntity e = new JobConfigEntity();
            e.setConfigKey("__global__");
            e.setContentYaml(contentYaml);
            jobConfigRepository.save(e);
            return;
        }
        JobConfig parsed = yamlConfigLoader.loadJobConfigFromString(contentYaml);
        if (parsed.getJob() == null || parsed.getJob().getName() == null || parsed.getJob().getName().trim().isEmpty()) {
            throw new IllegalArgumentException("YAML 内 job.name 不能为空，且作为配置唯一标识");
        }
        String jobName = parsed.getJob().getName().trim();
        if (currentKey == null || currentKey.trim().isEmpty()) {
            if (jobConfigRepository.existsByConfigKey(jobName)) {
                throw new IllegalArgumentException("job.name 已存在，不能重名: " + jobName);
            }
            JobConfigEntity e = new JobConfigEntity();
            e.setConfigKey(jobName);
            e.setContentYaml(contentYaml);
            jobConfigRepository.save(e);
            log.info("新增作业配置: {}", jobName);
            return;
        }
        String existingKey = currentKey.trim();
        if (jobName.equals(existingKey)) {
            JobConfigEntity e = new JobConfigEntity();
            e.setConfigKey(existingKey);
            e.setContentYaml(contentYaml);
            jobConfigRepository.save(e);
            log.debug("更新作业配置: {}", existingKey);
            return;
        }
        if (jobConfigRepository.existsByConfigKey(jobName)) {
            throw new IllegalArgumentException("job.name 已存在，不能重名: " + jobName);
        }
        jobConfigRepository.deleteByConfigKey(existingKey);
        JobConfigEntity e = new JobConfigEntity();
        e.setConfigKey(jobName);
        e.setContentYaml(contentYaml);
        jobConfigRepository.save(e);
        log.info("作业配置已重命名: {} -> {}", existingKey, jobName);
    }

    /** 删除作业配置。禁止删除 __global__。 */
    public void deleteJobConfig(String configKey) {
        if ("__global__".equals(configKey)) {
            throw new IllegalArgumentException("不能删除全局配置 __global__");
        }
        jobConfigRepository.deleteByConfigKey(configKey);
    }

    /**
     * 存量迁移：使 YAML 内 job.name 与 config_key 一致。
     * 对每条作业配置（不含 __global__），若 job.name 为空或与 config_key 不同，则把 job.name 设为 config_key 并回写 YAML。
     */
    public int migrateJobNameToConfigKey() throws IOException {
        List<JobConfigEntity> all = jobConfigRepository.findAll();
        int updated = 0;
        for (JobConfigEntity entity : all) {
            String key = entity.getConfigKey();
            if (key == null || "__global__".equals(key)) {
                continue;
            }
            JobConfig config = yamlConfigLoader.loadJobConfigFromString(entity.getContentYaml());
            if (config.getJob() == null) {
                config.setJob(new JobConfig.JobBasic());
            }
            String currentName = config.getJob().getName();
            if (currentName == null || currentName.trim().isEmpty() || !currentName.trim().equals(key)) {
                config.getJob().setName(key);
                String newYaml = yamlConfigLoader.writeJobConfigToString(config);
                entity.setContentYaml(newYaml);
                jobConfigRepository.save(entity);
                updated++;
                log.info("迁移 job.name 与 config_key 一致: config_key={}, 原 job.name={}", key, currentName);
            }
        }
        return updated;
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
