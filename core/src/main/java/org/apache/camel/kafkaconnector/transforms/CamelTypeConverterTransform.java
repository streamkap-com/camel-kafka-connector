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
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class CamelTypeConverterTransform<R extends ConnectRecord<R>> extends CamelTransformSupport<R> {

    public static final String FIELD_TARGET_TYPE_CONFIG = "target.type";
    public static final String ADD_DELETE_FIELD_CONFIG = "add.delete.field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_TARGET_TYPE_CONFIG, ConfigDef.Type.CLASS, null, ConfigDef.Importance.HIGH,
                    "The target field type to convert the value from, this is full qualified Java class, e.g: java.util.Map")
            .define(ADD_DELETE_FIELD_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Whether to add __delete field to the message based on HTTP DELETE method header");

    private static TypeConverter typeConverter;
    private Class<?> fieldTargetType;
    private boolean addDeleteField;

    // Cache for nested struct schemas to ensure consistent schema instances
    private final Map<String, Schema> schemaCache = new java.util.HashMap<>();

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        final Object convertedValue = convertValueWithCamelTypeConverter(value, record);
        final Schema updatedSchema = getOrBuildRecordSchema(schema, convertedValue);

        return newRecord(record, updatedSchema, convertedValue);
    }

    private Object convertValueWithCamelTypeConverter(final Object originalValue, R record) {
        // Handle null values
        if (originalValue == null) {
            return null;
        }

        // Special handling for JSON string to Map conversion
        if (Map.class.isAssignableFrom(fieldTargetType) && originalValue instanceof String) {
            String stringValue = (String) originalValue;

            // If empty string, create empty map and convert to struct
            if (stringValue.isEmpty()) {
                Map<String, Object> emptyMap = new java.util.LinkedHashMap<>();
                return mapToStruct(emptyMap, record);
            }

            // If not a JSON string, return as is
            if (!stringValue.startsWith("{")) {
                return stringValue;
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> mapValue = mapper.readValue((String) originalValue, Map.class);
                // Convert Map to Struct
                return mapToStruct(mapValue, record);
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
            return mapToStruct((Map<String, Object>) convertedValue, record);
        }

        return convertedValue;
    }


    private Struct mapToStruct(Map<String, Object> map, R record) {
        return mapToStruct(map, record, true);
    }

    private Struct mapToStruct(Map<String, Object> map, R record, boolean isTopLevel) {
        // First pass: Convert ISO 8601 date strings to Java Instant objects
        Map<String, Object> convertedMap = convertDateStringsInMap(map);

        // Add __deleted field only if config is enabled and field doesn't already exist (only for top-level)
        if (isTopLevel && addDeleteField && !convertedMap.containsKey("__deleted")) {
            boolean isDelete = isDeleteOperation(record);
            convertedMap.put("__deleted", isDelete);
        }

        // Generate schema name: use topic name for top-level, deterministic name for nested
        String schemaName;
        if (isTopLevel) {
            schemaName = record.topic();
        } else {
            schemaName = generateNestedStructSchemaName(convertedMap);
        }

        // Check if we have a cached schema for this name
        Schema cachedSchema = schemaCache.get(schemaName);
        if (cachedSchema != null && schemaHasAllFields(cachedSchema, convertedMap)) {
            Struct struct = new Struct(cachedSchema);
            // Populate struct with values
            for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Map && !(value instanceof java.util.Date)) {
                    value = mapToStruct((Map<String, Object>) value, record, false);
                } else if (value instanceof java.util.List) {
                    value = convertListElements((java.util.List<?>) value, record);
                }
                if (cachedSchema.field(entry.getKey()) != null) {
                    struct.put(entry.getKey(), value);
                }
            }
            return struct;
        }

        // Build schema for the first time
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(schemaName);

        // Build schema - recursively process nested structures to get actual schemas
        for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            Schema fieldSchema;
            // For nested maps, build the actual schema first by creating the struct
            if (value instanceof Map && !(value instanceof java.util.Date)) {
                Struct nestedStruct = mapToStruct((Map<String, Object>) value, record, false);
                fieldSchema = nestedStruct.schema();
            } else if (value instanceof java.util.List) {
                java.util.List<?> list = (java.util.List<?>) value;
                if (list.isEmpty()) {
                    fieldSchema = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
                } else {
                    Object firstElement = list.get(0);
                    if (firstElement instanceof Map && !(firstElement instanceof java.util.Date)) {
                        Struct nestedStruct = mapToStruct((Map<String, Object>) firstElement, record, false);
                        fieldSchema = SchemaBuilder.array(nestedStruct.schema()).optional().build();
                    } else {
                        Schema elementSchema = getFieldSchemaForType(firstElement);
                        fieldSchema = SchemaBuilder.array(elementSchema).optional().build();
                    }
                }
            } else {
                fieldSchema = getFieldSchemaForType(value);
            }

            schemaBuilder.field(key, fieldSchema);
        }

        Schema schema = schemaBuilder.build();
        schemaCache.put(schemaName, schema);

        Struct struct = new Struct(schema);

        // Populate struct with values
        for (Map.Entry<String, Object> entry : convertedMap.entrySet()) {
            Object value = entry.getValue();
            // Recursively convert nested Maps to Structs (pass false for isTopLevel)
            if (value instanceof Map && !(value instanceof java.util.Date)) {
                value = mapToStruct((Map<String, Object>) value, record, false);
            }
            // Convert list elements but keep as List
            else if (value instanceof java.util.List) {
                value = convertListElements((java.util.List<?>) value, record);
            }
            struct.put(entry.getKey(), value);
        }

        return struct;
    }

    /**
     * Check if a cached schema has all the fields needed for the current map.
     * Used to validate if we can reuse a cached schema.
     *
     * @param schema the cached schema
     * @param map the current map being processed
     * @return true if schema has all fields from map
     */
    private boolean schemaHasAllFields(Schema schema, Map<String, Object> map) {
        for (String key : map.keySet()) {
            if (schema.field(key) == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Convert elements within a Java List recursively.
     * Keeps the list structure intact (doesn't convert to array).
     * Recursively handles nested objects and arrays.
     *
     * @param list the list to process
     * @param record the source record (for nested struct conversion)
     * @return the list with converted elements
     */
    private java.util.List<?> convertListElements(java.util.List<?> list, R record) {
        if (list == null || list.isEmpty()) {
            return list;
        }

        java.util.List<Object> result = new java.util.ArrayList<>(list.size());
        for (Object item : list) {
            // Recursively convert nested Maps to Structs (nested, not top-level)
            if (item instanceof Map && !(item instanceof java.util.Date)) {
                result.add(mapToStruct((Map<String, Object>) item, record, false));
            }
            // Recursively convert nested Lists
            else if (item instanceof java.util.List) {
                result.add(convertListElements((java.util.List<?>) item, record));
            }
            else {
                result.add(item);
            }
        }
        return result;
    }

    private boolean isDeleteOperation(R record) {
        return record.headers() != null &&
                record.headers().lastWithName("CamelHeader.CamelHttpMethod") != null &&
                "DELETE".equals(record.headers().lastWithName("CamelHeader.CamelHttpMethod").value().toString().toUpperCase());
    }

    /**
     * Generate a deterministic schema name for nested structs based on their field names.
     * This ensures the same schema is used for the same structure across multiple messages.
     *
     * Example: For {"name": "John", "age": 30}, generates "NestedStruct_age_name"
     *
     * @param map the nested map
     * @return deterministic schema name based on sorted field names
     */
    private String generateNestedStructSchemaName(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return "NestedStruct_empty";
        }

        // Sort field names for consistency
        java.util.List<String> sortedKeys = new java.util.ArrayList<>(map.keySet());
        java.util.Collections.sort(sortedKeys);

        // Build schema name from sorted field names
        StringBuilder nameBuilder = new StringBuilder("NestedStruct");
        for (String key : sortedKeys) {
            nameBuilder.append("_").append(key);
        }

        return nameBuilder.toString();
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
     * Get a single-character type code for a value.
     * Used to create unique schema names based on data types.
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

        // Handle arrays
        if (value instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) value;
            if (list.isEmpty()) {
                // Empty array - default to array of strings
                return SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
            }
            // Get schema from first element
            Object firstElement = list.get(0);
            Schema elementSchema = getFieldSchemaForType(firstElement);
            return SchemaBuilder.array(elementSchema).optional().build();
        }

        // Handle nested Maps (nested JSON objects)
        // Return a simple struct schema with placeholder - the actual schema will come from mapToStruct()
        if (value instanceof Map && !(value instanceof java.util.Date)) {
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            String schemaName = generateNestedStructSchemaName(nestedMap);
            // Just return a struct schema with the correct name - fields will be validated at runtime
            return SchemaBuilder.struct().name(schemaName).optional().build();
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

        // If value is a Struct, use its schema directly - this is the actual schema being used
        if (value instanceof Struct) {
            return ((Struct) value).schema();
        }

        // If value is already properly typed, return the original schema
        if (originalSchema != null) {
            return originalSchema;
        }

        // Build schema from the value type if no original schema exists
        return SchemaHelper.buildSchemaBuilderForType(value).optional().build();
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
        addDeleteField = config.getBoolean(ADD_DELETE_FIELD_CONFIG);

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
