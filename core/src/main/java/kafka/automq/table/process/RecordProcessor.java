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

package kafka.automq.table.process;

import kafka.automq.table.process.exception.RecordProcessorException;

import org.apache.kafka.common.record.Record;

import java.util.List;
import java.util.Map;

/**
 * Processes Kafka records into standardized Avro format.
 * Handles format conversion, transformations, and error processing.
 */
public interface RecordProcessor {

    /**
     * Processes a Kafka record into Avro format.
     *
     * @param partition topic partition
     * @param record    the Kafka record to process
     * @return ProcessingResult containing the converted record or error information
     */
    ProcessingResult process(int partition, Record record);

    /**
     * Processes a Kafka record into zero or more rows, one per output row of the transform chain.
     *
     * <p>A failure is reported as a single failed result, so callers never see a partial set of rows.
     * An empty list means the record produced no rows.</p>
     *
     * @param partition topic partition
     * @param record    the Kafka record to process
     * @return one result per output row, or a single failed result
     */
    default List<ProcessingResult> processRows(int partition, Record record) {
        return List.of(process(partition, record));
    }


    void configure(Map<String, ?> configs) throws RecordProcessorException;
}
