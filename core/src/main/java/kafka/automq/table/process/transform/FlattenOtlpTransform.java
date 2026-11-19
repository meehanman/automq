/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.table.process.transform;

import kafka.automq.table.process.Transform;
import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;

/**
 * Expands an OTLP logs or traces export into one row per log record or span.
 *
 * <p>The root record selects the profile. A record with {@code resource_logs} uses the logs field names
 * ({@code resource_logs}, {@code scope_logs}, {@code log_records}). A record with {@code resource_spans} uses the
 * span field names ({@code resource_spans}, {@code scope_spans}, {@code spans}). Any other record fails.</p>
 *
 * <p>Each output row carries the leaf fields, the enclosing {@code resource} with {@code attributes} as an Avro map,
 * the enclosing {@code scope} unchanged, {@code scope_schema_url}, and the leaf {@code attributes} as an Avro map.
 * Map values keep the AnyValue record. Entries whose value is unset are dropped, and duplicate keys keep the last
 * value. {@code events}, {@code links}, and {@code body} are copied unchanged.</p>
 *
 * <p>Instances cache one output shape per input schema and are safe to share across threads.</p>
 */
public class FlattenOtlpTransform implements Transform {

    public static final FlattenOtlpTransform INSTANCE = new FlattenOtlpTransform();

    private static final int SHAPE_CACHE_MAX = 32;
    private static final String ROW_NAMESPACE = "kafka.automq.table.process";
    private static final String FIELD_ATTRIBUTES = "attributes";
    private static final String FIELD_RESOURCE = "resource";
    private static final String FIELD_SCOPE = "scope";
    private static final String FIELD_SCHEMA_URL = "schema_url";
    private static final String FIELD_SCOPE_SCHEMA_URL = "scope_schema_url";
    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";

    private static final Profile LOGS = new Profile("resource_logs", "scope_logs", "log_records", "OtlpLogRow");
    private static final Profile SPANS = new Profile("resource_spans", "scope_spans", "spans", "OtlpSpanRow");

    private final Map<Schema, Shape> shapeCache = new BoundedConcurrentHashMap<>(SHAPE_CACHE_MAX);

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration needed for this transform.
    }

    /**
     * Returns the single output row for an input with exactly one leaf record.
     *
     * @throws TransformException if the input does not expand to exactly one row, or is not an OTLP export
     */
    @Override
    public GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException {
        List<GenericRecord> rows = applyAll(record, context);
        if (rows.size() != 1) {
            throw new TransformException("flatten_otlp produced " + rows.size() + " rows; use applyAll for multi-row output");
        }
        return rows.get(0);
    }

    /**
     * Expands every resource, scope, and leaf element of the export into output rows, in input order.
     *
     * @return one row per leaf record; empty when every leaf array is empty
     * @throws TransformException if the record is null, or does not have a logs or spans export shape
     */
    @Override
    public List<GenericRecord> applyAll(GenericRecord record, TransformContext context) throws TransformException {
        if (record == null) {
            throw new TransformException("Record is null");
        }
        Shape shape = shapeCache.get(record.getSchema());
        if (shape == null) {
            shape = buildShape(record.getSchema());
            shapeCache.put(record.getSchema(), shape);
        }
        return expand(record, shape);
    }

    private static List<GenericRecord> expand(GenericRecord root, Shape shape) {
        Profile profile = shape.profile;
        List<GenericRecord> rows = new ArrayList<>();
        for (Object resourceElement : asCollection(root.get(profile.outerField))) {
            GenericRecord resourceLogs = (GenericRecord) resourceElement;
            GenericRecord resource = (GenericRecord) resourceLogs.get(FIELD_RESOURCE);
            GenericRecord resourceOut = resource == null ? null : toResourceRecord(resource, shape);
            for (Object scopeElement : asCollection(resourceLogs.get(profile.scopeField))) {
                GenericRecord scopeLogs = (GenericRecord) scopeElement;
                for (Object leafElement : asCollection(scopeLogs.get(profile.leafField))) {
                    rows.add(toRow((GenericRecord) leafElement, resourceOut, scopeLogs, shape));
                }
            }
        }
        return rows;
    }

    private static GenericRecord toRow(GenericRecord leaf, GenericRecord resourceOut, GenericRecord scopeLogs, Shape shape) {
        GenericRecord row = new GenericData.Record(shape.rowSchema);
        for (Schema.Field field : shape.rowSchema.getFields()) {
            switch (field.name()) {
                case FIELD_ATTRIBUTES:
                    row.put(field.name(), toAttributeMap(leaf.get(FIELD_ATTRIBUTES)));
                    break;
                case FIELD_RESOURCE:
                    row.put(field.name(), resourceOut);
                    break;
                case FIELD_SCOPE:
                    row.put(field.name(), scopeLogs.get(FIELD_SCOPE));
                    break;
                case FIELD_SCOPE_SCHEMA_URL:
                    row.put(field.name(), scopeLogs.get(FIELD_SCHEMA_URL));
                    break;
                default:
                    row.put(field.name(), leaf.get(field.name()));
                    break;
            }
        }
        return row;
    }

    private static GenericRecord toResourceRecord(GenericRecord resource, Shape shape) {
        GenericRecord out = new GenericData.Record(shape.resourceSchema);
        for (Schema.Field field : shape.resourceSchema.getFields()) {
            if (FIELD_ATTRIBUTES.equals(field.name())) {
                out.put(field.name(), toAttributeMap(resource.get(field.name())));
            } else {
                out.put(field.name(), resource.get(field.name()));
            }
        }
        return out;
    }

    private static Map<String, Object> toAttributeMap(Object keyValues) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Object element : asCollection(keyValues)) {
            GenericRecord keyValue = (GenericRecord) element;
            Object key = keyValue.get(FIELD_KEY);
            Object value = keyValue.get(FIELD_VALUE);
            if (key == null || value == null) {
                continue;
            }
            map.put(key.toString(), value);
        }
        return map;
    }

    private static Collection<?> asCollection(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        return (Collection<?>) value;
    }

    private static Shape buildShape(Schema root) throws TransformException {
        if (root.getType() != Schema.Type.RECORD) {
            throw new TransformException("flatten_otlp requires a record value, got " + root.getType());
        }
        Profile profile;
        if (root.getField(LOGS.outerField) != null) {
            profile = LOGS;
        } else if (root.getField(SPANS.outerField) != null) {
            profile = SPANS;
        } else {
            throw new TransformException("flatten_otlp requires an OTLP logs or traces export record");
        }

        Schema resourceLogs = arrayElement(root.getField(profile.outerField).schema());
        Schema resource = nonNull(resourceLogs.getField(FIELD_RESOURCE).schema());
        Schema scopeLogs = arrayElement(resourceLogs.getField(profile.scopeField).schema());
        Schema leaf = arrayElement(scopeLogs.getField(profile.leafField).schema());
        Schema keyValue = arrayElement(leaf.getField(FIELD_ATTRIBUTES).schema());
        Schema attributes = Schema.createMap(nonNull(keyValue.getField(FIELD_VALUE).schema()));

        Schema resourceSchema = Schema.createRecord(resource.getName(), resource.getDoc(), resource.getNamespace(), false);
        List<Schema.Field> resourceFields = new ArrayList<>();
        for (Schema.Field field : resource.getFields()) {
            Schema fieldSchema = FIELD_ATTRIBUTES.equals(field.name()) ? attributes : field.schema();
            resourceFields.add(new Schema.Field(field.name(), fieldSchema, field.doc(), null));
        }
        resourceSchema.setFields(resourceFields);

        Schema rowSchema = Schema.createRecord(profile.rowName, null, ROW_NAMESPACE, false);
        List<Schema.Field> rowFields = new ArrayList<>();
        for (Schema.Field field : leaf.getFields()) {
            Schema fieldSchema = FIELD_ATTRIBUTES.equals(field.name()) ? attributes : field.schema();
            rowFields.add(new Schema.Field(field.name(), fieldSchema, field.doc(), null));
        }
        Schema nullSchema = Schema.create(Schema.Type.NULL);
        rowFields.add(new Schema.Field(FIELD_RESOURCE, Schema.createUnion(Arrays.asList(nullSchema, resourceSchema))));
        rowFields.add(new Schema.Field(FIELD_SCOPE, scopeLogs.getField(FIELD_SCOPE).schema()));
        rowFields.add(new Schema.Field(FIELD_SCOPE_SCHEMA_URL, scopeLogs.getField(FIELD_SCHEMA_URL).schema()));
        rowSchema.setFields(rowFields);

        return new Shape(profile, rowSchema, resourceSchema);
    }

    private static Schema arrayElement(Schema schema) throws TransformException {
        Schema array = nonNull(schema);
        if (array.getType() != Schema.Type.ARRAY) {
            throw new TransformException("flatten_otlp expected an array, got " + array.getType());
        }
        return nonNull(array.getElementType());
    }

    private static Schema nonNull(Schema schema) throws TransformException {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        Schema nonNull = null;
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                continue;
            }
            if (nonNull != null) {
                throw new TransformException("flatten_otlp expected a nullable message, got " + schema);
            }
            nonNull = branch;
        }
        if (nonNull == null) {
            throw new TransformException("flatten_otlp expected a non-null message, got " + schema);
        }
        return nonNull;
    }

    private static final class Profile {
        private final String outerField;
        private final String scopeField;
        private final String leafField;
        private final String rowName;

        private Profile(String outerField, String scopeField, String leafField, String rowName) {
            this.outerField = outerField;
            this.scopeField = scopeField;
            this.leafField = leafField;
            this.rowName = rowName;
        }
    }

    private static final class Shape {
        private final Profile profile;
        private final Schema rowSchema;
        private final Schema resourceSchema;

        private Shape(Profile profile, Schema rowSchema, Schema resourceSchema) {
            this.profile = profile;
            this.rowSchema = rowSchema;
            this.resourceSchema = resourceSchema;
        }
    }
}
