package org.csits.kel.manager.plugin;

/**
 * 卸载作业插件接口，不同数据源/介质实现自己的导出逻辑。
 *
 * 为避免模块循环依赖，这里不直接依赖 TaskExecutionContext，
 * 而是由实现类自行决定接受的上下文类型（通常在 kel-server 中实现）。
 */
public interface ExtractPlugin {

    /**
     * 当前插件是否支持给定作业编码或作业类型（由实现自行解析上下文）。
     *
     * @param context 运行时上下文对象，一般为 TaskExecutionContext
     */
    boolean supports(Object context);

    /**
     * 执行卸载作业。
     *
     * @param context 运行时上下文对象，一般为 TaskExecutionContext
     */
    void extract(Object context) throws Exception;
}

