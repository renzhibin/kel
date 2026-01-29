package org.csits.kel.manager.plugin;

/**
 * 加载作业插件接口。
 */
public interface LoadPlugin {

    boolean supports(Object context);

    void load(Object context) throws Exception;
}

