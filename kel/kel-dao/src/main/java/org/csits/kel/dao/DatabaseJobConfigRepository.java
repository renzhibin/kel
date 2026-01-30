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
 * 基于数据库的配置仓储实现，读写 kel.job_config。
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class DatabaseJobConfigRepository implements JobConfigRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL =
        "INSERT INTO job_config (job_name, content_yaml) VALUES (?, ?)";
    private static final String UPDATE_SQL =
        "UPDATE job_config SET content_yaml = ? WHERE job_name = ?";
    private static final String SELECT_BY_KEY_SQL =
        "SELECT * FROM job_config WHERE job_name = ?";
    private static final String SELECT_ALL_SQL =
        "SELECT * FROM job_config ORDER BY job_name";
    private static final String DELETE_SQL =
        "DELETE FROM job_config WHERE job_name = ?";
    private static final String EXISTS_SQL =
        "SELECT 1 FROM job_config WHERE job_name = ? LIMIT 1";

    @Override
    public Optional<JobConfigEntity> findByConfigKey(String configKey) {
        List<JobConfigEntity> list = jdbcTemplate.query(SELECT_BY_KEY_SQL, new JobConfigRowMapper(), configKey);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public List<JobConfigEntity> findAll() {
        return jdbcTemplate.query(SELECT_ALL_SQL, new JobConfigRowMapper());
    }

    @Override
    public JobConfigEntity save(JobConfigEntity entity) {
        if (entity.getId() == null) {
            Optional<JobConfigEntity> existing = findByConfigKey(entity.getConfigKey());
            if (existing.isPresent()) {
                entity.setId(existing.get().getId());
                jdbcTemplate.update(UPDATE_SQL, entity.getContentYaml(), entity.getConfigKey());
                return entity;
            }
            KeyHolder keyHolder = new GeneratedKeyHolder();
            jdbcTemplate.update(connection -> {
                java.sql.PreparedStatement ps = connection.prepareStatement(INSERT_SQL, new String[]{"id"});
                ps.setString(1, entity.getConfigKey());
                ps.setString(2, entity.getContentYaml());
                return ps;
            }, keyHolder);
            if (keyHolder.getKey() != null) {
                entity.setId(keyHolder.getKey().longValue());
            }
        } else {
            jdbcTemplate.update(UPDATE_SQL, entity.getContentYaml(), entity.getConfigKey());
        }
        return entity;
    }

    @Override
    public void deleteByConfigKey(String configKey) {
        jdbcTemplate.update(DELETE_SQL, configKey);
    }

    @Override
    public boolean existsByConfigKey(String configKey) {
        List<Object> list = jdbcTemplate.query(EXISTS_SQL, (rs, rowNum) -> 1, configKey);
        return !list.isEmpty();
    }

    private static class JobConfigRowMapper implements RowMapper<JobConfigEntity> {
        @Override
        public JobConfigEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobConfigEntity e = new JobConfigEntity();
            e.setId(rs.getLong("id"));
            e.setConfigKey(rs.getString("job_name"));
            e.setContentYaml(rs.getString("content_yaml"));
            Timestamp created = rs.getTimestamp("created_at");
            e.setCreatedAt(created != null ? created.toLocalDateTime() : null);
            Timestamp updated = rs.getTimestamp("updated_at");
            e.setUpdatedAt(updated != null ? updated.toLocalDateTime() : null);
            return e;
        }
    }
}
