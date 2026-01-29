package org.csits.kel.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

/**
 * 基于数据库的任务执行仓储实现
 */
@Slf4j
@Repository
@ConditionalOnProperty(name = "kel.persistence.type", havingValue = "database", matchIfMissing = true)
@RequiredArgsConstructor
public class DatabaseTaskExecutionRepository implements TaskExecutionRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL =
        "INSERT INTO task_execution (job_code, batch_number, status, node_name, config_snapshot, " +
        "progress, current_stage, error_message, start_time, end_time, statistics) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String UPDATE_SQL =
        "UPDATE task_execution SET status = ?, node_name = ?, config_snapshot = ?, " +
        "progress = ?, current_stage = ?, error_message = ?, start_time = ?, end_time = ?, statistics = ? " +
        "WHERE id = ?";

    private static final String SELECT_BY_ID_SQL =
        "SELECT * FROM task_execution WHERE id = ?";

    private static final String SELECT_BY_JOB_CODE_SQL =
        "SELECT * FROM task_execution WHERE job_code = ? ORDER BY created_at DESC";

    private static final String SELECT_BY_BATCH_NUMBER_SQL =
        "SELECT * FROM task_execution WHERE batch_number = ? ORDER BY created_at DESC LIMIT 1";

    private static final String SELECT_ALL_SQL =
        "SELECT * FROM task_execution ORDER BY created_at DESC";

    private static final String DELETE_SQL =
        "DELETE FROM task_execution WHERE id = ?";

    private static final String COUNT_SQL =
        "SELECT COUNT(*) FROM task_execution";

    @Override
    public TaskExecutionEntity save(TaskExecutionEntity entity) {
        if (entity.getId() == null) {
            return insert(entity);
        } else {
            return update(entity);
        }
    }

    private TaskExecutionEntity insert(TaskExecutionEntity entity) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(connection -> {
            java.sql.PreparedStatement ps = connection.prepareStatement(INSERT_SQL, new String[]{"id"});
            ps.setString(1, entity.getJobCode());
            ps.setString(2, entity.getBatchNumber());
            ps.setString(3, entity.getStatus());
            ps.setString(4, entity.getNodeName());
            ps.setString(5, entity.getConfigSnapshot());
            ps.setInt(6, entity.getProgress());
            ps.setString(7, entity.getCurrentStage());
            ps.setString(8, entity.getErrorMessage());
            ps.setTimestamp(9, toTimestamp(entity.getStartTime()));
            ps.setTimestamp(10, toTimestamp(entity.getEndTime()));
            ps.setString(11, entity.getStatistics());
            return ps;
        }, keyHolder);

        Long id = keyHolder.getKey().longValue();
        entity.setId(id);
        log.debug("插入任务执行记录: id={}, jobCode={}, batchNumber={}",
            id, entity.getJobCode(), entity.getBatchNumber());
        return entity;
    }

    private TaskExecutionEntity update(TaskExecutionEntity entity) {
        int rows = jdbcTemplate.update(UPDATE_SQL,
            entity.getStatus(),
            entity.getNodeName(),
            entity.getConfigSnapshot(),
            entity.getProgress(),
            entity.getCurrentStage(),
            entity.getErrorMessage(),
            toTimestamp(entity.getStartTime()),
            toTimestamp(entity.getEndTime()),
            entity.getStatistics(),
            entity.getId()
        );

        if (rows == 0) {
            log.warn("更新任务执行记录失败，记录不存在: id={}", entity.getId());
        } else {
            log.debug("更新任务执行记录: id={}, status={}, progress={}",
                entity.getId(), entity.getStatus(), entity.getProgress());
        }
        return entity;
    }

    @Override
    public Optional<TaskExecutionEntity> findById(Long id) {
        List<TaskExecutionEntity> results = jdbcTemplate.query(
            SELECT_BY_ID_SQL,
            new TaskExecutionRowMapper(),
            id
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Override
    public List<TaskExecutionEntity> findByJobCode(String jobCode) {
        return jdbcTemplate.query(SELECT_BY_JOB_CODE_SQL, new TaskExecutionRowMapper(), jobCode);
    }

    @Override
    public Optional<TaskExecutionEntity> findByBatchNumber(String batchNumber) {
        List<TaskExecutionEntity> results = jdbcTemplate.query(
            SELECT_BY_BATCH_NUMBER_SQL,
            new TaskExecutionRowMapper(),
            batchNumber
        );
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Override
    public List<TaskExecutionEntity> findAll() {
        return jdbcTemplate.query(SELECT_ALL_SQL, new TaskExecutionRowMapper());
    }

    @Override
    public void deleteById(Long id) {
        int rows = jdbcTemplate.update(DELETE_SQL, id);
        if (rows > 0) {
            log.debug("删除任务执行记录: id={}", id);
        }
    }

    @Override
    public long count() {
        Long count = jdbcTemplate.queryForObject(COUNT_SQL, Long.class);
        return count != null ? count : 0;
    }

    /**
     * RowMapper实现
     */
    private static class TaskExecutionRowMapper implements RowMapper<TaskExecutionEntity> {
        @Override
        public TaskExecutionEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            TaskExecutionEntity entity = new TaskExecutionEntity();
            entity.setId(rs.getLong("id"));
            entity.setJobCode(rs.getString("job_code"));
            entity.setBatchNumber(rs.getString("batch_number"));
            entity.setStatus(rs.getString("status"));
            entity.setNodeName(rs.getString("node_name"));
            entity.setConfigSnapshot(rs.getString("config_snapshot"));
            entity.setProgress(rs.getInt("progress"));
            entity.setCurrentStage(rs.getString("current_stage"));
            entity.setErrorMessage(rs.getString("error_message"));
            entity.setStartTime(toLocalDateTime(rs.getTimestamp("start_time")));
            entity.setEndTime(toLocalDateTime(rs.getTimestamp("end_time")));
            entity.setStatistics(rs.getString("statistics"));
            entity.setCreatedAt(toLocalDateTime(rs.getTimestamp("created_at")));
            entity.setUpdatedAt(toLocalDateTime(rs.getTimestamp("updated_at")));
            return entity;
        }
    }

    /**
     * 转换LocalDateTime到Timestamp
     */
    private Timestamp toTimestamp(LocalDateTime dateTime) {
        return dateTime != null ? Timestamp.valueOf(dateTime) : null;
    }

    /**
     * 转换Timestamp到LocalDateTime
     */
    private static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp != null ? timestamp.toLocalDateTime() : null;
    }
}
