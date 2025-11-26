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
import java.time.format.DateTimeParseException;

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
        // First pass: Convert ISO 8601 date strings to Java Instant objects
        Map<String, Object> convertedMap = convertDateStringsInMap(map);

        // Generate unique schema name to force schema registry updates when structure changes
        String schemaName = generateDynamicSchemaName(convertedMap);
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(schemaName);

        // Build schema and populate struct in single iteration
        for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Determine schema based on value type
            Schema fieldSchema = getFieldSchemaForType(value);
            schemaBuilder.field(key, fieldSchema);
        }

        Schema schema = schemaBuilder.build();
        Struct struct = new Struct(schema);

        // Populate struct with values
        for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
            struct.put(entry.getKey(), entry.getValue());
        }

        return struct;
    }

    /**
     * Convert ISO 8601 date/time strings in the map to Java Instant objects.
     * This allows proper timestamp schema assignment instead of STRING schema.
     *
     * Detects and converts:
     * - 2025-10-24T16:53:52Z (ISO 8601 with Z)
     * - 2025-10-24T16:53:52.123Z (ISO 8601 with milliseconds and Z)
     * - 2025-10-24T16:53:52+00:00 (ISO 8601 with timezone offset)
     * - etc.
     *
     * @param map original map with string values
     * @return new map with ISO 8601 strings converted to java.time.Instant
     */
    private Map<String, Object> convertDateStringsInMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return map;
        }

        Map<String, Object> convertedMap = new java.util.LinkedHashMap<>(map);

        for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                Object convertedValue = tryConvertStringToDate((String) value);
                if (convertedValue != null) {
                    convertedMap.put(entry.getKey(), convertedValue);
                }
            }
        }

        return convertedMap;
    }

    /**
     * Try to parse a string as a date/time in multiple formats and convert to appropriate Java type.
     * Returns null if the string is not a valid date/time format.
     *
     * Converts to:
     * - java.util.Date for full timestamps (Kafka Connect Timestamp schema)
     * - java.time.LocalDate for date-only (Kafka Connect Date schema)
     * - java.time.LocalTime for time-only (Kafka Connect Time schema)
     *
     * Supported formats:
     * ISO 8601 Timestamps:
     * - 2025-10-24T16:53:52Z
     * - 2025-10-24T16:53:52.123Z
     * - 2025-10-24T16:53:52+00:00
     *
     * Date formats (with separators to avoid int conflicts):
     * - 2025-10-24 (yyyy-MM-dd)
     * - 24-10-2025 (dd-MM-yyyy)
     * - 10-24-2025 (MM-dd-yyyy)
     * - 2025/10/24 (yyyy/MM/dd)
     * - 24/10/2025 (dd/MM/yyyy)
     * - 10/24/2025 (MM/dd/yyyy)
     * - 24.10.2025 (dd.MM.yyyy)
     * - 2025.10.24 (yyyy.MM.dd)
     * - 24-Oct-2025 (dd-MMM-yyyy)
     * - Oct 24, 2025 (MMM dd, yyyy)
     * - October 24, 2025 (MMMM d, yyyy)
     *
     * Time formats:
     * - 16:53:52 (HH:mm:ss)
     * - 16:53:52.123 (HH:mm:ss.SSS)
     *
     * NOTE: Formats without separators (ddMMyyyy, yyyyMMdd) are intentionally excluded
     * to avoid conflicts with actual integer values in the data.
     *
     * @param value the string to parse
     * @return java.util.Date, java.time.LocalDate, or java.time.LocalTime if parseable, null otherwise
     */
    private Object tryConvertStringToDate(String value) {
        if (value == null || value.isEmpty() || value.length() < 8) {
            return null;
        }

        // Try full ISO 8601 timestamp first (contains T)
        if (value.contains("T")) {
            try {
                java.time.Instant instant = java.time.Instant.parse(value);
                return java.util.Date.from(instant);
            } catch (DateTimeParseException e) {
                return null;
            }
        }

        // Skip time-only format - keep as string
        // Return null so time values like "16:53:52" remain as STRING schema
        if (value.matches("\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?")) {
            return null;
        }

        // Try various date formats with separators (no time component)
        String[] datePatterns = {
                "yyyy-MM-dd",      // 2025-10-24
                "dd-MM-yyyy",      // 24-10-2025
                "MM-dd-yyyy",      // 10-24-2025
                "yyyy/MM/dd",      // 2025/10/24
                "dd/MM/yyyy",      // 24/10/2025
                "MM/dd/yyyy",      // 10/24/2025
                "yyyy.MM.dd",      // 2025.10.24
                "dd.MM.yyyy",      // 24.10.2025
                "MM.dd.yyyy",      // 10.24.2025
                "dd-MMM-yyyy",     // 24-Oct-2025
                "MMM dd, yyyy",    // Oct 24, 2025
                "MMMM d, yyyy"     // October 24, 2025
        };

        for (String pattern : datePatterns) {
            try {
                java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(pattern);
                java.time.LocalDate localDate = java.time.LocalDate.parse(value, formatter);
                // Convert LocalDate to days since epoch (1970-01-01)
                long daysSinceEpoch = localDate.toEpochDay();
                return (int) daysSinceEpoch;
            } catch (DateTimeParseException e) {
                continue;
            }
        }

        return null;
    }


    /**
     * Generate a unique schema name based on the map structure and data types.
     * This forces Schema Registry to create a new schema version when the data structure changes.
     *
     * Examples:
     * - {"id": 1, "name": "John"} → JsonData_i_s_v1
     * - {"id": 1, "name": "John", "active": true} → JsonData_i_s_b_v1
     *
     * @param map the data map
     * @return dynamic schema name with type signature
     */
    private String generateDynamicSchemaName(Map<String, Object> map) {
        StringBuilder typeSignature = new StringBuilder("JsonData");

        if (map == null || map.isEmpty()) {
            return typeSignature.append("_empty_v1").toString();
        }

        // Sort keys for consistent schema names
        map.keySet().stream()
           .sorted()
           .forEach(key -> {
               Object value = map.get(key);
               String typeCode = getTypeCode(value);
               typeSignature.append("_").append(typeCode);
           });

        typeSignature.append("_v1");
        return typeSignature.toString();
    }

    /**
     * Get a single-character type code for a value.
     * Used to create unique schema names based on data types.
     *
     * Type codes:
     * - s: String
     * - i: Integer
     * - l: Long
     * - b: Boolean
     * - d: Double
     * - f: Float
     * - h: Short
     * - y: Byte
     * - n: Number/BigDecimal
     * - t: Timestamp (java.util.Date, java.time.*)
     * - D: Date only (java.time.LocalDate, java.sql.Date)
     * - T: Time only (java.time.LocalTime, java.sql.Time)
     * - x: byte array
     * - ?: Unknown/null
     *
     * @param value the object value
     * @return type code character
     */
    private String getTypeCode(Object value) {
        if (value == null) {
            return "?";
        } else if (value instanceof String) {
            return "s";
        } else if (value instanceof Integer) {
            return "i";
        } else if (value instanceof Long) {
            return "l";
        } else if (value instanceof Boolean) {
            return "b";
        } else if (value instanceof Double) {
            return "d";
        } else if (value instanceof Float) {
            return "f";
        } else if (value instanceof Short) {
            return "h";
        } else if (value instanceof Byte) {
            return "y";
        } else if (value instanceof java.math.BigDecimal) {
            return "n";
        } else if (value instanceof java.time.LocalDate || value instanceof java.sql.Date) {
            return "D";
        } else if (value instanceof java.time.LocalTime || value instanceof java.sql.Time) {
            return "T";
        } else if (value instanceof java.util.Date || value instanceof java.time.Instant ||
                   value instanceof java.time.ZonedDateTime || value instanceof java.time.LocalDateTime ||
                   value instanceof java.sql.Timestamp) {
            return "t";
        } else if (value instanceof byte[]) {
            return "x";
        } else {
            return "?";
        }
    }

    private Schema getFieldSchemaForType(Object value) {
        if (value == null) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }

        // Use instanceof pattern matching for type checking (Java 16+)
        if (value instanceof String) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else if (value instanceof Float) {
            return Schema.OPTIONAL_FLOAT32_SCHEMA;
        } else if (value instanceof Short) {
            return Schema.OPTIONAL_INT16_SCHEMA;
        } else if (value instanceof Byte) {
            return Schema.OPTIONAL_INT8_SCHEMA;
        } else if (value instanceof java.math.BigDecimal) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        } else if (value instanceof java.time.Instant) {
            // java.time.Instant represents a point in time
            return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
        } else if (value instanceof java.time.ZonedDateTime) {
            // java.time.ZonedDateTime represents date, time and zone
            return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
        } else if (value instanceof java.time.LocalDateTime) {
            // java.time.LocalDateTime represents date and time
            return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
        } else if (value instanceof java.time.LocalDate) {
            // java.time.LocalDate represents date only
            return org.apache.kafka.connect.data.Date.builder().optional().build();
        } else if (value instanceof java.time.LocalTime) {
            // java.time.LocalTime represents time only
            return org.apache.kafka.connect.data.Time.builder().optional().build();
        } else if (value instanceof java.sql.Timestamp) {
            // java.sql.Timestamp represents timestamp with nanosecond precision
            // Must check BEFORE java.sql.Date since Timestamp extends java.sql.Date
            return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
        } else if (value instanceof java.sql.Time) {
            // java.sql.Time represents time only (no date component)
            return org.apache.kafka.connect.data.Time.builder().optional().build();
        } else if (value instanceof java.sql.Date) {
            // java.sql.Date represents date only (no time component)
            return org.apache.kafka.connect.data.Date.builder().optional().build();
        } else if (value instanceof java.util.Date) {
            // java.util.Date represents timestamp with millisecond precision
            // Check this LAST since it's the parent class for sql.Date and sql.Timestamp
            return org.apache.kafka.connect.data.Timestamp.builder().optional().build();
        } else if (value instanceof byte[]) {
            return Schema.OPTIONAL_BYTES_SCHEMA;
        } else {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
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
