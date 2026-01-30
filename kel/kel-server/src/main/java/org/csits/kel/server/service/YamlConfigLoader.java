package org.csits.kel.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.GlobalConfig;
import org.csits.kel.server.dto.JobConfig;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * 使用 Jackson YAML 将配置文件映射为 Java 对象。
 */
@Slf4j
@Component
public class YamlConfigLoader {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

    public GlobalConfig loadGlobalConfig(Resource resource) throws IOException {
        try (InputStream in = resource.getInputStream()) {
            return yamlMapper.readValue(in, GlobalConfig.class);
        }
    }

    public JobConfig loadJobConfig(Resource resource) throws IOException {
        try (InputStream in = resource.getInputStream()) {
            return yamlMapper.readValue(in, JobConfig.class);
        }
    }

    public GlobalConfig loadGlobalConfigFromString(String yaml) throws IOException {
        return yamlMapper.readValue(new StringReader(yaml), GlobalConfig.class);
    }

    public JobConfig loadJobConfigFromString(String yaml) throws IOException {
        return yamlMapper.readValue(new StringReader(yaml), JobConfig.class);
    }
}

