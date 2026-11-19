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

package kafka.automq.table.worker;

import kafka.automq.table.process.DataError;
import kafka.automq.table.process.ProcessingResult;
import kafka.automq.table.process.RecordProcessor;
import kafka.automq.table.process.exception.RecordProcessorException;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.server.record.ErrorsTolerance;

import com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("S3Unit")
class IcebergWriterMultiRowTest {

    private InMemoryCatalog catalog;
    private TableIdentifier tableId;
    private WorkerConfig config;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", ImmutableMap.of());
        catalog.createNamespace(Namespace.of("default"));
        tableId = TableIdentifier.of("default", "multi-row");
        config = mock(WorkerConfig.class);
        when(config.partitionBy()).thenReturn(List.of());
        when(config.errorsTolerance()).thenReturn(ErrorsTolerance.NONE);
    }

    /**
     * Given one Kafka record that expands to two rows,
     * when the writer accepts it, then both rows are written and the partition offset advances once.
     */
    @Test
    void twoRowsAdvanceTheOffsetOnce() throws IOException, RecordProcessorException {
        CountingWriter writer = writer(List.of(row("a"), row("b")));
        writer.setOffset(0, 10L);

        writer.write(0, kafkaRecord(10L));

        assertEquals(2, writer.writes);
        assertEquals(11L, writer.getOffset(0).end());
    }

    /**
     * Given one Kafka record whose second row fails processing,
     * when the writer accepts it, then no row is written and the offset stays put.
     */
    @Test
    void failedRowWritesNothingAndLeavesTheOffset() {
        ProcessingResult failure = new ProcessingResult(new DataError(DataError.ErrorType.DATA_ERROR, "bad row", null));
        CountingWriter writer = writer(List.of(row("a"), failure));
        writer.setOffset(0, 10L);

        assertThrows(IOException.class, () -> writer.write(0, kafkaRecord(10L)));

        assertEquals(0, writer.writes);
        assertEquals(10L, writer.getOffset(0).end());
    }

    private CountingWriter writer(List<ProcessingResult> rows) {
        RecordProcessor processor = mock(RecordProcessor.class);
        doReturn(rows).when(processor).processRows(anyInt(), any());
        IcebergTableManager tableManager = new IcebergTableManager(catalog, tableId, config);
        return new CountingWriter(tableManager, processor, config);
    }

    private static ProcessingResult row(String name) {
        Schema schema = Schema.createRecord("Leaf", null, null, false);
        schema.setFields(List.of(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", name);
        return new ProcessingResult(record, schema, "leaf");
    }

    private static Record kafkaRecord(long offset) {
        Record record = mock(Record.class);
        when(record.offset()).thenReturn(offset);
        when(record.timestamp()).thenReturn(1L);
        when(record.sizeInBytes()).thenReturn(8);
        return record;
    }

    private static final class CountingWriter extends IcebergWriter {
        private int writes;

        private CountingWriter(IcebergTableManager tableManager, RecordProcessor processor, WorkerConfig config) {
            super(tableManager, processor, config);
        }

        @Override
        protected void writeRow(int partition, Record kafkaRecord, ProcessingResult result) throws IOException, RecordProcessorException {
            super.writeRow(partition, kafkaRecord, result);
            writes++;
        }
    }
}
