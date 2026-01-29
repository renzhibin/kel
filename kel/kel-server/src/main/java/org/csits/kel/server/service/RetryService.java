package org.csits.kel.server.service;

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.csits.kel.server.dto.GlobalConfig;
import org.springframework.stereotype.Service;

/**
 * 重试服务
 * 实现基于配置的自动重试机制
 */
@Slf4j
@Service
public class RetryService {

    /**
     * 执行带重试的操作
     *
     * @param operation 要执行的操作
     * @param retryConfig 重试配置
     * @param operationName 操作名称（用于日志）
     * @param <T> 返回类型
     * @return 操作结果
     * @throws Exception 所有重试失败后抛出最后一次异常
     */
    public <T> T executeWithRetry(Supplier<T> operation, GlobalConfig.RetryConfig retryConfig,
                                   String operationName) throws Exception {
        int maxRetries = retryConfig != null && retryConfig.getMaxRetries() != null
            ? retryConfig.getMaxRetries() : 0;
        int retryInterval = retryConfig != null && retryConfig.getRetryIntervalSec() != null
            ? retryConfig.getRetryIntervalSec() : 60;

        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    log.info("重试 {} (第 {}/{} 次)", operationName, attempt, maxRetries);
                }
                return operation.get();
            } catch (Exception e) {
                lastException = e;
                log.warn("{} 失败 (第 {}/{} 次): {}", operationName, attempt + 1, maxRetries + 1, e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        log.info("等待 {} 秒后重试...", retryInterval);
                        Thread.sleep(retryInterval * 1000L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new Exception("重试被中断", ie);
                    }
                }
            }
        }

        log.error("{} 失败，已达到最大重试次数 {}", operationName, maxRetries);
        throw lastException;
    }

    /**
     * 执行带重试的void操作
     *
     * @param operation 要执行的操作
     * @param retryConfig 重试配置
     * @param operationName 操作名称
     * @throws Exception 所有重试失败后抛出最后一次异常
     */
    public void executeWithRetryVoid(RunnableWithException operation, GlobalConfig.RetryConfig retryConfig,
                                     String operationName) throws Exception {
        executeWithRetry(() -> {
            try {
                operation.run();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, retryConfig, operationName);
    }

    /**
     * 可抛出异常的Runnable接口
     */
    @FunctionalInterface
    public interface RunnableWithException {
        void run() throws Exception;
    }
}
