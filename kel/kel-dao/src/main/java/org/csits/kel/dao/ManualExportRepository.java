package org.csits.kel.dao;

import java.util.List;
import java.util.Optional;

/**
 * 人工表级导出记录仓储，读写 kel.manual_export。
 */
public interface ManualExportRepository {

    ManualExportEntity save(ManualExportEntity entity);

    Optional<ManualExportEntity> findById(Long id);

    List<ManualExportEntity> findByJobName(String jobName);

    List<ManualExportEntity> findByJobNameAndTableName(String jobName, String tableName);

    List<ManualExportEntity> findAll(int page, int size);

    long count();
}
