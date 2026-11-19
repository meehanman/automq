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

import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

/**
 * Transform interface for Avro GenericRecord data processing.
 *
 * <p>Stateless, chainable transformations for content manipulation like CDC unwrapping,
 * field mapping, and data enrichment. Implementations must be thread-safe.</p>
 *
 * @see Converter
 * @see TransformException
 */
public interface Transform {

    /**
     * Configures the transform with operation-specific settings.
     *
     * @param configs configuration parameters, must not be null
     * @throws IllegalArgumentException if configs is null or contains invalid values
     */
    void configure(Map<String, ?> configs);

    /**
     * Applies transformation to the input GenericRecord.
     *
     * @param record the input GenericRecord, must not be null
     * @param context contextual information for the transformation
     * @return the transformed GenericRecord, must not be null
     * @throws TransformException if transformation fails
     * @throws IllegalArgumentException if record or context is null
     */
    GenericRecord apply(GenericRecord record, TransformContext context) throws TransformException;

    /**
     * Applies transformation to one input record and returns every output row it produces.
     *
     * <p>The default implementation wraps {@link #apply(GenericRecord, TransformContext)} in a single-element
     * list. Transforms that fan out one input into many rows override this method. An empty list means the
     * input produced no rows, and the Kafka record still counts as processed.</p>
     *
     * @param record the input GenericRecord, must not be null
     * @param context contextual information for the transformation
     * @return the output rows, never null
     * @throws TransformException if transformation fails
     */
    default List<GenericRecord> applyAll(GenericRecord record, TransformContext context) throws TransformException {
        GenericRecord output = apply(record, context);
        if (output == null) {
            throw new TransformException("Transform " + getName() + " returned null record");
        }
        return List.of(output);
    }

    /**
     * Returns a descriptive name for this transform.
     *
     * @return transform name, never null
     */
    default String getName() {
        return this.getClass().getSimpleName();
    }
}
