package org.csits.kel.web;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 测试用配置，供 @WebMvcTest 发现 @SpringBootConfiguration。
 */
@SpringBootApplication(scanBasePackages = "org.csits.kel.web")
public class WebTestApplication {
}
