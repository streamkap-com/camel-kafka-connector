/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.kafkaconnector.transforms;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.TypeConverter;
import org.apache.camel.kafkaconnector.utils.SchemaHelper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class CamelTypeConverterTransform<R extends ConnectRecord<R>> extends CamelTransformSupport<R> {

    public static final String FIELD_TARGET_TYPE_CONFIG = "target.type";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_TARGET_TYPE_CONFIG, ConfigDef.Type.CLASS, null, ConfigDef.Importance.HIGH,
                    "The target field type to convert the value from, this is full qualified Java class, e.g: java.util.Map");

    private static TypeConverter typeConverter;
    private Class<?> fieldTargetType;

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        final Object convertedValue = convertValueWithCamelTypeConverter(value);
        final Schema updatedSchema = getOrBuildRecordSchema(schema, convertedValue);

        return newRecord(record, updatedSchema, convertedValue);
    }

    private Object convertValueWithCamelTypeConverter(final Object originalValue) {
        // Handle null values
        if (originalValue == null) {
            return null;
        }

        // Special handling for JSON string to Map conversion
        if (Map.class.isAssignableFrom(fieldTargetType) && originalValue instanceof String) {
            String stringValue = (String) originalValue;

            // If not a JSON string, return as is
            if (!stringValue.startsWith("{")) {
                return stringValue;
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> mapValue = mapper.readValue((String) originalValue, Map.class);
                // Convert Map to Struct
                return mapToStruct(mapValue);
            } catch (Exception e) {
                throw new DataException("Failed to parse JSON string to Map: " + e.getMessage(), e);
            }
        }
        final Object convertedValue = typeConverter.tryConvertTo(fieldTargetType, originalValue);

        if (convertedValue == null) {
            throw new DataException(String.format("CamelTypeConverter was not able to convert value `%s` to target type of `%s`", originalValue, fieldTargetType.getSimpleName()));
        }

        // If the converted value is a Map, convert it to Struct
        if (convertedValue instanceof Map) {
            return mapToStruct((Map<String, Object>) convertedValue);
        }

        return convertedValue;
    }


    private Struct mapToStruct(Map<String, Object> map) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("JsonData");

        for (String key : map.keySet()) {
            Object value = map.get(key);
            if (value instanceof String) {
                schemaBuilder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            } else if (value instanceof Integer) {
                schemaBuilder.field(key, Schema.OPTIONAL_INT32_SCHEMA);
            } else if (value instanceof Long) {
                schemaBuilder.field(key, Schema.OPTIONAL_INT64_SCHEMA);
            } else if (value instanceof Boolean) {
                schemaBuilder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (value instanceof Double) {
                schemaBuilder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else {
                schemaBuilder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }
        }

        Schema schema = schemaBuilder.build();
        Struct struct = new Struct(schema);

        for (String key : map.keySet()) {
            struct.put(key, map.get(key));
        }

        return struct;
    }

    private Schema getOrBuildRecordSchema(final Schema originalSchema, final Object value) {
        // Handle null values
        if (value == null) {
            return originalSchema != null ? originalSchema : Schema.OPTIONAL_STRING_SCHEMA;
        }
        // If value is a Struct, use its schema directly
        if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(originalSchema, SchemaHelper.buildSchemaBuilderForType(value));

        if (originalSchema.isOptional()) {
            builder.optional();
        }
        if (originalSchema.defaultValue() != null) {
            builder.defaultValue(convertValueWithCamelTypeConverter(originalSchema.defaultValue()));
        }

        return builder.build();
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
        fieldTargetType = config.getClass(FIELD_TARGET_TYPE_CONFIG);

        if (fieldTargetType == null) {
            throw new ConfigException("Configuration 'target.type' can not be empty!");
        }

        // initialize type converter from camel context
        typeConverter = getCamelContext().getTypeConverter();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static final class Key<R extends ConnectRecord<R>> extends CamelTypeConverterTransform<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            Object keyValue = record.key();

            // If key is a JSON string, parse it
            if (keyValue instanceof String && ((String) keyValue).startsWith("{")) {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue((String) keyValue, Map.class);
                } catch (Exception e) {
                    throw new DataException("Failed to parse key JSON: " + e.getMessage(), e);
                }
            }

            return keyValue;
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends CamelTypeConverterTransform<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
