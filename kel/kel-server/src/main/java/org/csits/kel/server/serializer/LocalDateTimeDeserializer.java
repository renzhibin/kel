package org.csits.kel.server.serializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.time.LocalDateTime;

/**
 * 支持从 ISO 字符串或旧格式对象（year/monthValue/dayOfMonth/hour/minute/second/nano）反序列化为 LocalDateTime，
 * 兼容已持久化的 statistics JSON。
 */
public class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

    @Override
    public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonNode node = p.getCodec().readTree(p);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return LocalDateTime.parse(node.asText());
        }
        if (node.isObject()) {
            int year = node.path("year").asInt();
            int month = node.path("monthValue").asInt(1);
            int day = node.path("dayOfMonth").asInt(1);
            int hour = node.path("hour").asInt(0);
            int minute = node.path("minute").asInt(0);
            int second = node.path("second").asInt(0);
            int nano = node.path("nano").asInt(0);
            return LocalDateTime.of(year, month, day, hour, minute, second, nano);
        }
        return null;
    }
}
