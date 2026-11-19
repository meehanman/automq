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

import kafka.automq.table.process.TransformContext;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Tag("S3Unit")
class FlattenOtlpTransformTest {

    private static final String KEY_VALUE_DEF = "{'type':'record','name':'opentelemetry.proto.common.v1.KeyValue','fields':["
        + "{'name':'key','type':'string'},"
        + "{'name':'value','type':['null',{'type':'record','name':'opentelemetry.proto.common.v1.AnyValue','fields':["
        + "{'name':'string_value','type':['null','string'],'default':null},"
        + "{'name':'int_value','type':['null','long'],'default':null},"
        + "{'name':'bool_value','type':['null','boolean'],'default':null}]}],'default':null}]}";

    private static final String LOG_RECORD = "{'type':'record','name':'opentelemetry.proto.logs.v1.LogRecord','fields':["
        + "{'name':'severity_text','type':'string'},"
        + "{'name':'body','type':['null','opentelemetry.proto.common.v1.AnyValue'],'default':null},"
        + "{'name':'attributes','type':{'type':'array','items':'opentelemetry.proto.common.v1.KeyValue'}}]}";

    private static final String SCOPE_LOGS = "{'type':'record','name':'opentelemetry.proto.logs.v1.ScopeLogs','fields':["
        + "{'name':'scope','type':['null',{'type':'record','name':'opentelemetry.proto.common.v1.InstrumentationScope','fields':["
        + "{'name':'name','type':'string'},"
        + "{'name':'attributes','type':{'type':'array','items':'opentelemetry.proto.common.v1.KeyValue'}}]}],'default':null},"
        + "{'name':'log_records','type':{'type':'array','items':" + LOG_RECORD + "}},"
        + "{'name':'schema_url','type':'string'}]}";

    private static final String RESOURCE_LOGS = "{'type':'record','name':'opentelemetry.proto.logs.v1.ResourceLogs','fields':["
        + "{'name':'resource','type':['null',{'type':'record','name':'opentelemetry.proto.resource.v1.Resource','fields':["
        + "{'name':'attributes','type':{'type':'array','items':" + KEY_VALUE_DEF + "}},"
        + "{'name':'dropped_attributes_count','type':'int'}]}],'default':null},"
        + "{'name':'scope_logs','type':{'type':'array','items':" + SCOPE_LOGS + "}},"
        + "{'name':'schema_url','type':'string'}]}";

    private static final String LOGS_ROOT = "{'type':'record','name':'opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest','fields':["
        + "{'name':'resource_logs','type':{'type':'array','items':" + RESOURCE_LOGS + "}}]}";

    private static final String SPAN_EVENT = "{'type':'record','name':'opentelemetry.proto.trace.v1.Span.Event','fields':["
        + "{'name':'name','type':'string'},"
        + "{'name':'attributes','type':{'type':'array','items':'opentelemetry.proto.common.v1.KeyValue'}}]}";

    private static final String SPAN_LINK = "{'type':'record','name':'opentelemetry.proto.trace.v1.Span.Link','fields':["
        + "{'name':'trace_id','type':'string'},"
        + "{'name':'attributes','type':{'type':'array','items':'opentelemetry.proto.common.v1.KeyValue'}}]}";

    private static final String SPAN = "{'type':'record','name':'opentelemetry.proto.trace.v1.Span','fields':["
        + "{'name':'trace_id','type':'string'},"
        + "{'name':'name','type':'string'},"
        + "{'name':'attributes','type':{'type':'array','items':'opentelemetry.proto.common.v1.KeyValue'}},"
        + "{'name':'events','type':{'type':'array','items':" + SPAN_EVENT + "}},"
        + "{'name':'links','type':{'type':'array','items':" + SPAN_LINK + "}}]}";

    private static final String SCOPE_SPANS = "{'type':'record','name':'opentelemetry.proto.trace.v1.ScopeSpans','fields':["
        + "{'name':'scope','type':['null',{'type':'record','name':'opentelemetry.proto.common.v1.InstrumentationScope','fields':["
        + "{'name':'name','type':'string'}]}],'default':null},"
        + "{'name':'spans','type':{'type':'array','items':" + SPAN + "}},"
        + "{'name':'schema_url','type':'string'}]}";

    private static final String RESOURCE_SPANS = "{'type':'record','name':'opentelemetry.proto.trace.v1.ResourceSpans','fields':["
        + "{'name':'resource','type':['null',{'type':'record','name':'opentelemetry.proto.resource.v1.Resource','fields':["
        + "{'name':'attributes','type':{'type':'array','items':" + KEY_VALUE_DEF + "}}]}],'default':null},"
        + "{'name':'scope_spans','type':{'type':'array','items':" + SCOPE_SPANS + "}},"
        + "{'name':'schema_url','type':'string'}]}";

    private static final String TRACE_ROOT = "{'type':'record','name':'opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest','fields':["
        + "{'name':'resource_spans','type':{'type':'array','items':" + RESOURCE_SPANS + "}}]}";

    private FlattenOtlpTransform transform;
    private TransformContext context;

    @BeforeEach
    void setUp() {
        transform = new FlattenOtlpTransform();
        context = mock(TransformContext.class);
    }

    /**
     * Given two resources, each with one scope and two log records,
     * when the export is expanded, then every log record becomes its own row
     * and carries the resource of its own parent.
     */
    @Test
    void logsExpandEveryResourceScopeAndLogRecord() throws TransformException {
        Logs f = new Logs(parse(LOGS_ROOT));
        GenericRecord request = request(f,
            resourceLogs(f, "svc-a", scopeLogs(f, "url-a", logRecord(f, "A1"), logRecord(f, "A2"))),
            resourceLogs(f, "svc-b", scopeLogs(f, "url-b", logRecord(f, "B1"), logRecord(f, "B2"))));

        List<GenericRecord> rows = transform.applyAll(request, context);

        assertEquals(4, rows.size());
        assertEquals(List.of("svc-a", "svc-a", "svc-b", "svc-b"),
            rows.stream().map(FlattenOtlpTransformTest::serviceName).collect(Collectors.toList()));
    }

    /**
     * Given one log record with a string body and two attributes,
     * when the export is expanded, then the attributes become an Avro map of AnyValue records,
     * the body stays a record, and scope_schema_url comes from the enclosing scope.
     */
    @Test
    void logsRowKeepsBodyAndMapsAttributes() throws TransformException {
        Logs f = new Logs(parse(LOGS_ROOT));
        GenericRecord request = request(f,
            resourceLogs(f, "svc-a", scopeLogs(f, "url-a", logRecord(f, "hello", "k1", "v1", "k2", "v2"))));

        List<GenericRecord> rows = transform.applyAll(request, context);

        assertEquals(1, rows.size());
        GenericRecord row = rows.get(0);
        Map<?, ?> attributes = (Map<?, ?>) row.get("attributes");
        assertEquals(2, attributes.size());
        assertEquals("v1", ((GenericRecord) attributes.get("k1")).get("string_value").toString());
        GenericRecord body = (GenericRecord) row.get("body");
        assertNotNull(body);
        assertEquals("hello", body.get("string_value").toString());
        assertEquals("url-a", row.get("scope_schema_url").toString());
        assertNull(row.getSchema().getField("keyStrindex"));
    }

    /**
     * Given a scope whose log_records array is empty,
     * when the export is expanded, then no rows are produced for that branch.
     */
    @Test
    void emptyLogRecordsProduceNoRows() throws TransformException {
        Logs f = new Logs(parse(LOGS_ROOT));
        GenericRecord request = request(f, resourceLogs(f, "svc-a", scopeLogs(f, "url-a")));

        assertTrue(transform.applyAll(request, context).isEmpty());
    }

    /**
     * Given attributes with a repeated key and a key whose value is unset,
     * when the export is expanded, then the repeated key keeps its last value
     * and the unset entry is dropped.
     */
    @Test
    void duplicateKeysKeepLastValueAndUnsetValuesAreDropped() throws TransformException {
        Logs f = new Logs(parse(LOGS_ROOT));
        GenericRecord request = request(f,
            resourceLogs(f, "svc-a", scopeLogs(f, "url-a", logRecord(f, "m", "a", "first", "a", "second", "b", null))));

        GenericRecord row = transform.applyAll(request, context).get(0);

        Map<?, ?> attributes = (Map<?, ?>) row.get("attributes");
        assertEquals(1, attributes.size());
        assertEquals("second", ((GenericRecord) attributes.get("a")).get("string_value").toString());
    }

    /**
     * Given an export with exactly one log record,
     * when apply is used, then it returns that single row,
     * and given an export with two log records, apply rejects it so callers use applyAll.
     */
    @Test
    void applyReturnsSingleRowOnlyWhenExactlyOneRowIsProduced() throws TransformException {
        Logs f = new Logs(parse(LOGS_ROOT));
        GenericRecord single = request(f, resourceLogs(f, "svc-a", scopeLogs(f, "url-a", logRecord(f, "one"))));
        GenericRecord two = request(f, resourceLogs(f, "svc-a",
            scopeLogs(f, "url-a", logRecord(f, "one"), logRecord(f, "two"))));

        assertEquals("svc-a", serviceName(transform.apply(single, context)));
        assertThrows(TransformException.class, () -> transform.apply(two, context));
    }

    /**
     * Given a traces export with one resource and one scope holding two spans, each with an event and a link,
     * when the export is expanded, then each span becomes one row and events and links stay lists.
     */
    @Test
    void spansExpandEachSpanAndKeepEventsAndLinksNested() throws TransformException {
        Schema root = parse(TRACE_ROOT);
        Schema resourceSpans = items(root, "resource_spans");
        Schema resource = nested(resourceSpans, "resource");
        Schema keyValue = items(resource, "attributes");
        Schema scopeSpans = items(resourceSpans, "scope_spans");
        Schema span = items(scopeSpans, "spans");
        Schema event = items(span, "events");
        Schema link = items(span, "links");

        GenericRecord spanA = rec(span, "trace_id", "t1", "name", "op-a",
            "attributes", attrs(keyValue, "http.method", "GET"),
            "events", List.of(rec(event, "name", "e1", "attributes", attrs(keyValue, "k", "v"))),
            "links", List.of(rec(link, "trace_id", "t2", "attributes", attrs(keyValue))));
        GenericRecord spanB = rec(span, "trace_id", "t1", "name", "op-b",
            "attributes", attrs(keyValue),
            "events", List.of(rec(event, "name", "e2", "attributes", attrs(keyValue))),
            "links", List.of(rec(link, "trace_id", "t3", "attributes", attrs(keyValue))));
        GenericRecord scopeRecord = rec(scopeSpans, "scope", null, "spans", List.of(spanA, spanB), "schema_url", "");
        GenericRecord resourceRecord = rec(resourceSpans, "resource",
            rec(resource, "attributes", attrs(keyValue, "service.name", "svc-span")),
            "scope_spans", List.of(scopeRecord), "schema_url", "");
        GenericRecord request = rec(root, "resource_spans", List.of(resourceRecord));

        List<GenericRecord> rows = transform.applyAll(request, context);

        assertEquals(2, rows.size());
        for (GenericRecord row : rows) {
            assertTrue(row.get("events") instanceof Collection);
            assertEquals(1, ((Collection<?>) row.get("events")).size());
            assertTrue(row.get("links") instanceof Collection);
            assertEquals(1, ((Collection<?>) row.get("links")).size());
            assertEquals("svc-span", serviceName(row));
        }
        assertEquals("op-a", rows.get(0).get("name").toString());
        assertTrue(((Map<?, ?>) rows.get(0).get("attributes")).containsKey("http.method"));
    }

    /**
     * Given a record with neither resource_logs nor resource_spans,
     * when the transform is applied, then it fails with a TransformException.
     */
    @Test
    void recordWithoutLogsOrSpansFieldsIsRejected() {
        Schema other = parse("{'type':'record','name':'Other','fields':[{'name':'x','type':'int'}]}");
        GenericRecord record = rec(other, "x", 1);

        assertThrows(TransformException.class, () -> transform.applyAll(record, context));
    }

    private static final class Logs {
        private final Schema root;
        private final Schema resourceLogs;
        private final Schema resource;
        private final Schema scopeLogs;
        private final Schema logRecord;
        private final Schema keyValue;
        private final Schema scope;

        private Logs(Schema root) {
            this.root = root;
            this.resourceLogs = items(root, "resource_logs");
            this.resource = nested(resourceLogs, "resource");
            this.scopeLogs = items(resourceLogs, "scope_logs");
            this.logRecord = items(scopeLogs, "log_records");
            this.keyValue = items(resource, "attributes");
            this.scope = nested(scopeLogs, "scope");
        }
    }

    private static GenericRecord request(Logs f, GenericRecord... resourceLogs) {
        return rec(f.root, "resource_logs", List.of(resourceLogs));
    }

    private static GenericRecord resourceLogs(Logs f, String service, GenericRecord... scopeLogRecords) {
        GenericRecord resource = rec(f.resource, "attributes", attrs(f.keyValue, "service.name", service),
            "dropped_attributes_count", 0);
        return rec(f.resourceLogs, "resource", resource, "scope_logs", List.of(scopeLogRecords), "schema_url", "");
    }

    private static GenericRecord scopeLogs(Logs f, String schemaUrl, GenericRecord... logRecords) {
        GenericRecord scope = rec(f.scope, "name", "scope", "attributes", List.of());
        return rec(f.scopeLogs, "scope", scope, "log_records", List.of(logRecords), "schema_url", schemaUrl);
    }

    private static GenericRecord logRecord(Logs f, String body, Object... keyValuePairs) {
        return logRecordWith(f, body, attrs(f.keyValue, keyValuePairs));
    }

    private static GenericRecord logRecordWith(Logs f, String body, List<GenericRecord> attributes) {
        GenericRecord bodyValue = anyString(f.logRecord, body);
        return rec(f.logRecord, "severity_text", "INFO", "body", bodyValue, "attributes", attributes);
    }

    private static GenericRecord anyString(Schema logRecord, String value) {
        return rec(nested(logRecord, "body"), "string_value", value);
    }

    private static List<GenericRecord> attrs(Schema keyValue, Object... keyValuePairs) {
        Schema anyValue = nested(keyValue, "value");
        List<GenericRecord> out = new ArrayList<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            String key = (String) keyValuePairs[i];
            String value = (String) keyValuePairs[i + 1];
            GenericRecord any = value == null ? null : rec(anyValue, "string_value", value);
            out.add(rec(keyValue, "key", key, "value", any));
        }
        return out;
    }

    private static String serviceName(GenericRecord row) {
        Map<?, ?> attributes = (Map<?, ?>) ((GenericRecord) row.get("resource")).get("attributes");
        return ((GenericRecord) attributes.get("service.name")).get("string_value").toString();
    }

    private static GenericRecord rec(Schema schema, Object... nameValuePairs) {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            record.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return record;
    }

    private static Schema parse(String json) {
        return new Schema.Parser().parse(json.replace('\'', '"'));
    }

    private static Schema items(Schema parent, String field) {
        return parent.getField(field).schema().getElementType();
    }

    private static Schema nested(Schema parent, String field) {
        return unwrap(parent.getField(field).schema());
    }

    private static Schema unwrap(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        throw new IllegalStateException("No non-null branch in " + schema);
    }
}
