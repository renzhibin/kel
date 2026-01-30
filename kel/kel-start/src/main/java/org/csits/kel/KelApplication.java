package org.csits.kel;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.TaskExecutionContext;
import org.csits.kel.server.service.JobConfigService;
import org.csits.kel.server.service.TaskExecutionService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 启动类，通过命令行参数触发卸载/加载任务。
 *
 * 示例：
 *  java -jar kel-start.jar --jobName=bss
 */
@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "org.csits.kel")
@RequiredArgsConstructor
public class KelApplication implements CommandLineRunner {

    private final JobConfigService jobConfigService;
    private final TaskExecutionService taskExecutionService;

    public static void main(String[] args) {
        SpringApplication.run(KelApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String jobName = null;
        for (String arg : args) {
            if (arg.startsWith("--jobName=")) {
                jobName = arg.substring("--jobName=".length());
            }
        }
        if (jobName == null || jobName.isEmpty()) {
            log.info("未指定 jobName，以常驻模式启动");
            return;
        }
        JobConfigService.MergedResult mergedConfig =
            jobConfigService.loadMergedConfig(jobName);
        TaskExecutionContext context = taskExecutionService.createContext(
            jobName,
            mergedConfig.getGlobalConfig(),
            mergedConfig.getJobConfig()
        );
        switch (mergedConfig.getJobConfig().getJob().getType()) {
            case EXTRACT_KINGBASE:
            case FILE_EXTRACT:
                taskExecutionService.executeExtract(context);
                break;
            case KINGBASE_LOAD:
            case FILE_LOAD:
                taskExecutionService.executeLoad(context);
                break;
            default:
                log.warn("不支持的作业类型 type={}, jobName={}",
                    mergedConfig.getJobConfig().getJob().getType(), jobName);
        }
    }
}

