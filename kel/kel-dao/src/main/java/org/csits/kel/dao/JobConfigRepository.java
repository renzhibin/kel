package org.csits.kel.dao;

import java.util.List;
import java.util.Optional;

/**
 * 配置表仓储，读写 kel.job_config。
 */
public interface JobConfigRepository {

    Optional<JobConfigEntity> findByConfigKey(String configKey);

    List<JobConfigEntity> findAll();

    JobConfigEntity save(JobConfigEntity entity);

    void deleteByConfigKey(String configKey);

    boolean existsByConfigKey(String configKey);
}
