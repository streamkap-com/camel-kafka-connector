package org.apache.camel.kafkaconnector.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class CamelDynamicTopicTransform<R extends ConnectRecord<R>> extends CamelTransformSupport<R> {

    public static final String HEADER_NAME_CONFIG = "header.name";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Header name to extract and use as the record topic");

    private String headerName;

    @Override
    public R apply(R record) {
        String topicName = record.topic();

        // Extract topic from header if configured
        if (headerName != null && !headerName.isEmpty() && record.headers() != null) {
            Header header = record.headers().lastWithName(headerName);
            if (header != null && header.value() != null) {
                String headerValue = header.value().toString();
                headerValue = extractPathFromUri(headerValue);
                if (!headerValue.trim().isEmpty()) {
                    topicName = headerValue;
                }
            }
        }

        return newRecord(record, topicName);
    }

    private String extractPathFromUri(String uri) {
        try {
            java.net.URL url = new java.net.URL(uri);
            String path = url.getPath();
            return path.replaceAll("^/+|/+$", "").replaceAll("/", "_");
        } catch (Exception e) {
            return uri.replaceAll("^/+|/+$", "").replaceAll("/", "_");
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        headerName = config.getString(HEADER_NAME_CONFIG);

        if (headerName == null || headerName.isEmpty()) {
            throw new ConfigException("Configuration 'header.name' can not be empty!");
        }
    }

    protected abstract R newRecord(R record, String topicName);

    public static final class Value<R extends ConnectRecord<R>> extends CamelDynamicTopicTransform<R> {
        @Override
        protected R newRecord(R record, String topicName) {
            return record.newRecord(topicName, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }
    }
}
