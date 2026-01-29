package org.csits.kel.job;

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.JobConfigService;
import org.csits.kel.server.service.TaskExecutionService;
import org.springframework.stereotype.Component;

/**
 * 提供给 xxl-job 的 kel 任务处理器。
 *
 * 调度中心配置示例：
 * - JobHandler：kelJobHandler
 * - 执行参数（executorParam）：直接填写作业编码，如：
 *   - "bss_kingbase_extract"
 *   - "bss_kingbase_load"
 *   - "bss_file_extract"
 *   - "bss_file_load"
 *   具体执行逻辑由 YAML 中 `job.type`（见 JobType 枚举）决定。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KelXxlJobHandler {

    private final JobConfigService jobConfigService;
    private final TaskExecutionService taskExecutionService;

    @XxlJob("kelJobHandler")
    public void execute() throws Exception {
        String param = XxlJobHelper.getJobParam();
        XxlJobHelper.log("kelJobHandler start, param={}", param);

        if (param == null || param.trim().isEmpty()) {
            XxlJobHelper.handleFail("缺少参数，需提供 jobCode，例如：bss_kingbase_extract");
            return;
        }
        String jobCode = param.trim();
        if (jobCode.isEmpty()) {
            XxlJobHelper.handleFail("缺少 jobCode，参数示例：bss_kingbase_extract");
            return;
        }

        try {
            JobConfigService.MergedResult merged =
                jobConfigService.loadMergedConfig(jobCode);
            TaskExecutionContext context = taskExecutionService.createContext(
                jobCode,
                merged.getGlobalConfig(),
                merged.getJobConfig()
            );
            switch (merged.getJobConfig().getJob().getType()) {
                case EXTRACT_KINGBASE:
                case FILE_EXTRACT:
                    taskExecutionService.executeExtract(context);
                    break;
                case KINGBASE_LOAD:
                case FILE_LOAD:
                    taskExecutionService.executeLoad(context);
                    break;
                default:
                    String msg = "不支持的作业类型 type=" + merged.getJobConfig().getJob().getType()
                        + ", jobCode=" + jobCode;
                    XxlJobHelper.log(msg);
                    XxlJobHelper.handleFail(msg);
                    return;
            }
            XxlJobHelper.log("kelJobHandler success, jobCode={}, type={}",
                jobCode, merged.getJobConfig().getJob().getType());
        } catch (Exception e) {
            log.error("kelJobHandler failed, param={}", param, e);
            XxlJobHelper.log(e);
            XxlJobHelper.handleFail("kelJobHandler 执行失败：" + e.getMessage());
            throw e;
        }
    }
}

