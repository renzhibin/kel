package org.csits.kel.server.dto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.csits.kel.server.constants.ExtractType;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.constants.LoadMode;

/**
 * 作业配置，对应单个卸载/加载 YAML。
 * 支持新格式（job / settings / resources / tasks）与旧格式（顶层 extract_database、extract_tasks 等）兼容。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {

    private JobBasic job;

    /** 新格式：2. 全局配置 (Settings) */
    private Settings settings;

    /** 新格式：3. 资源配置 (Resources) */
    private Resources resources;

    /** 新格式：4. 任务流水线 (Tasks)，统一列表，按 mode 区分卸载/加载任务 */
    private List<TaskItem> tasks;

    // ---------- 旧格式字段（解析时仍可写入，getter 由下方兼容方法提供） ----------
    @Getter(AccessLevel.NONE)
    @JsonProperty("extract_database")
    private ExtractDatabaseConfig extractDatabase;

    @Getter(AccessLevel.NONE)
    @JsonProperty("target_database")
    private TargetDatabaseConfig targetDatabase;

    @Getter(AccessLevel.NONE)
    private String workDir;

    @Getter(AccessLevel.NONE)
    private String exchangeDir;

    @Getter(AccessLevel.NONE)
    @JsonProperty("extract_tasks")
    private List<ExtractTaskConfig> extractTasks;

    @Getter(AccessLevel.NONE)
    @JsonProperty("load_tasks")
    private List<LoadTaskConfig> loadTasks;

    @Getter(AccessLevel.NONE)
    private RuntimeConfig runtime;

    @Getter(AccessLevel.NONE)
    @JsonProperty("input_directory")
    private String inputDirectory;

    @Getter(AccessLevel.NONE)
    private String extractDirectory;

    @Getter(AccessLevel.NONE)
    private String targetDirectory;

    // ---------- 兼容 getter：优先从新结构取值 ----------
    public ExtractDatabaseConfig getExtractDatabase() {
        if (resources != null && resources.getExtractDatabase() != null) {
            return resources.getExtractDatabase();
        }
        return extractDatabase;
    }

    public TargetDatabaseConfig getTargetDatabase() {
        if (resources != null && resources.getTargetDatabase() != null) {
            return resources.getTargetDatabase();
        }
        return targetDatabase;
    }

    public String getWorkDir() {
        if (settings != null && settings.getWorkDir() != null) {
            return settings.getWorkDir();
        }
        return workDir;
    }

    public String getExchangeDir() {
        if (resources != null && resources.getExtractDir() != null && resources.getExtractDir().getExchangeDir() != null) {
            return resources.getExtractDir().getExchangeDir();
        }
        return exchangeDir;
    }

    public RuntimeConfig getRuntime() {
        if (settings != null && settings.getRuntime() != null) {
            return settings.getRuntime();
        }
        return runtime;
    }

    public String getInputDirectory() {
        if (resources != null && resources.getLoadDir() != null && resources.getLoadDir().getInputDir() != null) {
            return resources.getLoadDir().getInputDir();
        }
        return inputDirectory;
    }

    public String getExtractDirectory() {
        if (resources != null && resources.getExtractDir() != null && resources.getExtractDir().getExtractDir() != null) {
            return resources.getExtractDir().getExtractDir();
        }
        return extractDirectory;
    }

    public String getTargetDirectory() {
        if (resources != null && resources.getLoadDir() != null && resources.getLoadDir().getTargetDir() != null) {
            return resources.getLoadDir().getTargetDir();
        }
        return targetDirectory;
    }

    /** 从统一 tasks 中筛出卸载任务（mode=FULL/INCREMENTAL）并转为 ExtractTaskConfig 列表。 */
    public List<ExtractTaskConfig> getExtractTasks() {
        if (tasks != null && !tasks.isEmpty()) {
            List<ExtractTaskConfig> list = new ArrayList<>();
            for (TaskItem t : tasks) {
                String mode = t.getMode();
                if (mode == null) {
                    continue;
                }
                if ("FULL".equalsIgnoreCase(mode) || "INCREMENTAL".equalsIgnoreCase(mode)) {
                    ExtractTaskConfig etc = new ExtractTaskConfig();
                    etc.setType("FULL".equalsIgnoreCase(mode) ? ExtractType.FULL : ExtractType.INCREMENTAL);
                    etc.setTables(t.getTables());
                    etc.setSqlList(t.getQueries());
                    etc.setInterfaceMapping(t.getInterfaceMapping());
                    if (t.getFiles() != null) {
                        FileAttribute attr = new FileAttribute();
                        attr.setFilePattern(t.getFiles().getPattern());
                        attr.setTimeType(t.getFiles().getTimeType());
                        attr.setTimeRange(t.getFiles().getTimeRange());
                        attr.setFileSizeLimitMb(t.getFiles().getSizeLimitMb());
                        etc.setAttribute(attr);
                    }
                    list.add(etc);
                }
            }
            if (!list.isEmpty()) {
                return list;
            }
        }
        return extractTasks != null ? extractTasks : Collections.emptyList();
    }

    /** 从统一 tasks 中筛出加载任务（mode=TRUNCATE_LOAD/MERGE/APPEND）并转为 LoadTaskConfig 列表。 */
    public List<LoadTaskConfig> getLoadTasks() {
        if (tasks != null && !tasks.isEmpty()) {
            List<LoadTaskConfig> list = new ArrayList<>();
            for (TaskItem t : tasks) {
                String mode = t.getMode();
                if (mode == null) {
                    continue;
                }
                if ("TRUNCATE_LOAD".equalsIgnoreCase(mode) || "MERGE".equalsIgnoreCase(mode) || "APPEND".equalsIgnoreCase(mode)) {
                    LoadTaskConfig ltc = new LoadTaskConfig();
                    ltc.setType(LoadMode.valueOf(mode.toUpperCase()));
                    ltc.setInterfaceMapping(t.getMappings());
                    ltc.setEnableTransaction(t.getTransaction());
                    if (t.getMergeSql() != null && !t.getMergeSql().trim().isEmpty()) {
                        SqlItem item = new SqlItem();
                        item.setName("merge_sql");
                        item.setSql(t.getMergeSql());
                        ltc.setSqlList(Collections.singletonList(item));
                    }
                    list.add(ltc);
                }
            }
            if (!list.isEmpty()) {
                return list;
            }
        }
        return loadTasks != null ? loadTasks : Collections.emptyList();
    }

    // ---------- 新格式内嵌类型 ----------

    /** 2. 全局配置 (Settings) */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Settings {

        private RuntimeConfig runtime;

        private String encoding;

        @JsonProperty("batch_number_format")
        private String batchNumberFormat;

        @JsonProperty("work_dir")
        private String workDir;

        @JsonProperty("cleanup_work_dir")
        private Boolean cleanupWorkDir;

        private CompressionBlock compression;

        private SecurityBlock security;

        @JsonProperty("disk_protection")
        private DiskProtectionBlock diskProtection;

        @JsonProperty("file_naming")
        private FileNamingBlock fileNaming;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CompressionBlock {

        @JsonProperty("enable_compression")
        private Boolean enableCompression;

        private String algorithm;

        @JsonProperty("split_threshold_gb")
        private Double splitThresholdGb;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SecurityBlock {

        @JsonProperty("enable_encryption")
        private Boolean enableEncryption;

        @JsonProperty("sm4_key")
        private String sm4Key;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DiskProtectionBlock {

        private Boolean enabled;

        @JsonProperty("min_free_space_gb")
        private Double minFreeSpaceGb;

        @JsonProperty("min_free_space_percent")
        private Double minFreeSpacePercent;

        @JsonProperty("fail_on_check_error")
        private Boolean failOnCheckError;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FileNamingBlock {

        @JsonProperty("enable_standard_naming")
        private Boolean enableStandardNaming;

        private String version;
    }

    /** 3. 资源配置 (Resources) */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Resources {

        @JsonProperty("extract_database")
        private ExtractDatabaseConfig extractDatabase;

        @JsonProperty("target_database")
        private TargetDatabaseConfig targetDatabase;

        /** YAML: extract_dir.extract_dir / exchange_dir */
        @JsonProperty("extract_dir")
        private ExtractDir extractDir;

        /** YAML: load_dir.input_dir / target_dir */
        @JsonProperty("load_dir")
        private LoadDir loadDir;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExtractDir {

        /** YAML: extract_dir（与父 key 同名） */
        @JsonProperty("extract_dir")
        private String extractDir;

        @JsonProperty("exchange_dir")
        private String exchangeDir;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LoadDir {

        @JsonProperty("input_dir")
        private String inputDir;

        @JsonProperty("target_dir")
        private String targetDir;
    }

    /** 4. 任务项（卸载/加载统一用 mode 区分） */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TaskItem {

        private String mode;

        private List<String> tables;

        private List<SqlItem> queries;

        private TaskFileConfig files;

        @JsonProperty("interface_mapping")
        private Map<String, String> interfaceMapping;

        private Map<String, String> mappings;

        @JsonProperty("merge_sql")
        private String mergeSql;

        private Boolean transaction;

        @JsonProperty("target_dir")
        private String targetDir;
    }

    /** 文件采集配置（tasks[].files） */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TaskFileConfig {

        private String pattern;

        @JsonProperty("time_type")
        private String timeType;

        @JsonProperty("time_range")
        private String timeRange;

        @JsonProperty("size_limit_mb")
        private Integer sizeLimitMb;
    }

    // ---------- 原有内嵌类型（继续供插件与合并逻辑使用） ----------

    @Data
    public static class JobBasic {

        @JsonProperty("type")
        private JobType type;

        @JsonProperty("name")
        private String name;

        @JsonProperty("description")
        private String description;
    }

    @Data
    public static class ExtractDatabaseConfig {

        private String host;

        private Integer port;

        private String name;

        private String user;

        private String password;

        @JsonProperty("database_version")
        @JsonAlias("version")
        private String databaseVersion;
    }

    @Data
    public static class TargetDatabaseConfig {

        private String host;

        private Integer port;

        private String name;

        private String user;

        private String password;

        @JsonProperty("database_version")
        @JsonAlias("version")
        private String databaseVersion;
    }

    @Data
    public static class ExtractTaskConfig {

        private ExtractType type;

        private List<String> tables;

        private List<SqlItem> sqlList;

        private Map<String, String> interfaceMapping;

        private FileAttribute attribute;
    }

    @Data
    public static class SqlItem {

        private String name;

        private String sql;
    }

    @Data
    public static class FileAttribute {

        private String method;

        private String filePattern;

        private String timeType;

        private String timeRange;

        private Integer fileSizeLimitMb;
    }

    @Data
    public static class LoadTaskConfig {

        private LoadMode type;

        private Map<String, String> interfaceMapping;

        private List<SqlItem> sqlList;

        private Boolean enableTransaction;
    }

    @Data
    public static class RuntimeConfig {

        @JsonProperty("table_concurrency")
        private Integer tableConcurrency;

        @JsonProperty("max_retries")
        private Integer maxRetries;

        @JsonProperty("retry_interval_sec")
        private Integer retryIntervalSec;
    }
}
