package org.csits.kel.server.dto;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.csits.kel.server.constants.ExtractType;
import org.csits.kel.server.constants.JobType;
import org.csits.kel.server.constants.LoadMode;

/**
 * 作业配置，对应单个卸载/加载 YAML。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {

    private JobBasic job;

    @JsonProperty("extract_database")
    private ExtractDatabaseConfig extractDatabase;

    private TargetDatabaseConfig targetDatabase;

    private String workDir;

    private String exchangeDir;

    @JsonProperty("extract_tasks")
    private List<ExtractTaskConfig> extractTasks;

    @JsonProperty("load_tasks")
    private List<LoadTaskConfig> loadTasks;

    private RuntimeConfig runtime;

    private String inputDirectory;

    private String extractDirectory;

    private String targetDirectory;

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

        @JsonProperty("host")
        private String host;

        @JsonProperty("port")
        private Integer port;

        @JsonProperty("name")
        private String name;

        @JsonProperty("user")
        private String user;

        @JsonProperty("password")
        private String password;

        @JsonProperty("version")
        private String version;
    }

    @Data
    public static class TargetDatabaseConfig {

        private String host;

        private Integer port;

        private String name;

        private String user;

        private String password;
    }

    @Data
    public static class ExtractTaskConfig {

        private ExtractType type;

        /**
         * 全量表清单。
         */
        private List<String> tables;

        /**
         * SQL 配置列表，用于增量任务。
         */
        private List<SqlItem> sqlList;

        /**
         * 接口映射，例如表名/任务名 到 接口编号。
         */
        private Map<String, String> interfaceMapping;

        /**
         * 非结构化文件采集属性。
         */
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

        /**
         * 接口映射，键为接口编号或表名，值为目标表。
         */
        private Map<String, String> interfaceMapping;

        private List<SqlItem> sqlList;

        /**
         * 是否启用事务。
         */
        private Boolean enableTransaction;
    }

    @Data
    public static class RuntimeConfig {

        /**
         * 作业内并发度，覆盖全局默认值。
         */
        @JsonProperty("table_concurrency")
        private Integer tableConcurrency;
    }
}

