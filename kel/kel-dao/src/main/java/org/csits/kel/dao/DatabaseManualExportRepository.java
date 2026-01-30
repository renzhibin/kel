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
        "INSERT INTO manual_export (job_code, table_name, mode, status, task_id, requested_by) " +
        "VALUES (?, ?, ?, ?, ?, ?)";
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
                ps.setString(1, entity.getJobCode());
                ps.setString(2, entity.getTableName());
                ps.setString(3, entity.getMode());
                ps.setString(4, entity.getStatus());
                ps.setObject(5, entity.getTaskId());
                ps.setString(6, entity.getRequestedBy());
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
            e.setJobCode(rs.getString("job_code"));
            e.setTableName(rs.getString("table_name"));
            e.setMode(rs.getString("mode"));
            e.setStatus(rs.getString("status"));
            long taskId = rs.getLong("task_id");
            e.setTaskId(rs.wasNull() ? null : taskId);
            Timestamp at = rs.getTimestamp("requested_at");
            e.setRequestedAt(at != null ? at.toLocalDateTime() : null);
            e.setRequestedBy(rs.getString("requested_by"));
            return e;
        }
    }
}
