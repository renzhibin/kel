package org.csits.kel.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * 基于数据库的人工表级导出记录仓储实现，读写 kel.manual_export。
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class DatabaseManualExportRepository implements ManualExportRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL =
        "INSERT INTO manual_export (type, job_code, table_name, mode, source_batch, status, task_id, requested_by) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_SQL =
        "UPDATE manual_export SET status = ?, task_id = ? WHERE id = ?";
    private static final String SELECT_BY_ID_SQL =
        "SELECT * FROM manual_export WHERE id = ?";
    private static final String SELECT_BY_JOB_SQL =
        "SELECT * FROM manual_export WHERE job_code = ? ORDER BY requested_at DESC";
    private static final String SELECT_BY_JOB_AND_TABLE_SQL =
        "SELECT * FROM manual_export WHERE job_code = ? AND table_name = ? ORDER BY requested_at DESC";
    private static final String SELECT_ALL_SQL =
        "SELECT * FROM manual_export ORDER BY requested_at DESC LIMIT ? OFFSET ?";
    private static final String COUNT_SQL =
        "SELECT COUNT(*) FROM manual_export";

    @Override
    public ManualExportEntity save(ManualExportEntity entity) {
        if (entity.getId() == null) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                java.sql.PreparedStatement ps = connection.prepareStatement(INSERT_SQL, new String[]{"id"});
                String type = entity.getType() != null ? entity.getType() : "EXPORT";
                ps.setString(1, type);
                ps.setString(2, entity.getJobCode());
                ps.setString(3, entity.getTableName());
                ps.setString(4, entity.getMode());
                ps.setString(5, entity.getSourceBatch());
                ps.setString(6, entity.getStatus());
                ps.setObject(7, entity.getTaskId());
                ps.setString(8, entity.getRequestedBy());
                return ps;
            }, keyHolder);
            if (keyHolder.getKey() != null) {
                entity.setId(keyHolder.getKey().longValue());
            }
        } else {
            jdbcTemplate.update(UPDATE_SQL, entity.getStatus(), entity.getTaskId(), entity.getId());
        }
        return entity;
    }

    @Override
    public Optional<ManualExportEntity> findById(Long id) {
        List<ManualExportEntity> list = jdbcTemplate.query(SELECT_BY_ID_SQL, new ManualExportRowMapper(), id);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public List<ManualExportEntity> findByJobCode(String jobCode) {
        return jdbcTemplate.query(SELECT_BY_JOB_SQL, new ManualExportRowMapper(), jobCode);
    }

    @Override
    public List<ManualExportEntity> findByJobCodeAndTableName(String jobCode, String tableName) {
        return jdbcTemplate.query(SELECT_BY_JOB_AND_TABLE_SQL, new ManualExportRowMapper(), jobCode, tableName);
    }

    @Override
    public List<ManualExportEntity> findAll(int page, int size) {
        int offset = page * size;
        return jdbcTemplate.query(SELECT_ALL_SQL, new ManualExportRowMapper(), size, offset);
    }

    @Override
    public long count() {
        Long n = jdbcTemplate.queryForObject(COUNT_SQL, Long.class);
        return n != null ? n : 0;
    }

    private static class ManualExportRowMapper implements RowMapper<ManualExportEntity> {
        @Override
        public ManualExportEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            ManualExportEntity e = new ManualExportEntity();
            e.setId(rs.getLong("id"));
            e.setType(hasColumn(rs, "type") ? nullToDefault(rs.getString("type"), "EXPORT") : "EXPORT");
            e.setJobCode(rs.getString("job_code"));
            e.setTableName(rs.getString("table_name"));
            e.setMode(rs.getString("mode"));
            e.setSourceBatch(hasColumn(rs, "source_batch") ? rs.getString("source_batch") : null);
            e.setStatus(rs.getString("status"));
            long taskId = rs.getLong("task_id");
            e.setTaskId(rs.wasNull() ? null : taskId);
            Timestamp at = rs.getTimestamp("requested_at");
            e.setRequestedAt(at != null ? at.toLocalDateTime() : null);
            e.setRequestedBy(rs.getString("requested_by"));
            return e;
        }

        private static String nullToDefault(String v, String def) {
            return v != null && !v.isEmpty() ? v : def;
        }

        private static boolean hasColumn(ResultSet rs, String columnName) throws SQLException {
            java.sql.ResultSetMetaData meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                if (columnName.equalsIgnoreCase(meta.getColumnName(i))) return true;
            }
            return false;
        }
    }
}
