/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.truth.AbstractFailureStrategy;
import com.google.common.truth.TestVerb;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;

/**
 * Given set of field names, creates a copy of protobuf object, with only the indicated fields included.
 */
public final class ProtobufExt {

    private ProtobufExt() {
    }

    public static <T extends Message> T copy(T entity, Set<String> fields) {
        // Split nested field names
        Map<String, Set<String>> topNames = PropertiesExt.splitNames(fields, 1);

        // Filter
        Message.Builder builder = entity.toBuilder();

        // As we modify the builder, we need to take a snapshot this
        Set<Descriptors.FieldDescriptor> fieldDescriptors = new HashSet<>(builder.getAllFields().keySet());
        for (Descriptors.FieldDescriptor field : fieldDescriptors) {
            if (!topNames.containsKey(field.getName())) {
                builder.clearField(field);
            } else {
                Set<String> nested = topNames.get(field.getName());
                if (nested != null) {
                    Object value = builder.getField(field);
                    if (value != null) {
                        if (value instanceof Message) {
                            Message messageValue = (Message) value;
                            if (!messageValue.getAllFields().isEmpty()) {
                                builder.setField(field, copy(messageValue, nested));
                            }
                        } else if (value instanceof Collection) {
                            Collection<?> collection = (Collection<?>) value;
                            if (!collection.isEmpty()) {
                                Object first = CollectionsExt.first(collection);
                                if (first instanceof MapEntry) {
                                    if (((MapEntry) first).getKey() instanceof String) {
                                        List<?> filteredMap = collection.stream().filter(item -> {
                                            MapEntry<String, Object> entry = (MapEntry<String, Object>) item;
                                            return nested.contains(entry.getKey());
                                        }).collect(Collectors.toList());
                                        builder.setField(field, filteredMap);
                                    }
                                } else if (first instanceof Message) {
                                    Iterator<?> it = collection.iterator();
                                    int size = collection.size();
                                    for (int i = 0; i < size; i++) {
                                        builder.setRepeatedField(field, i, copy((Message) it.next(), nested));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return (T) builder.build();
    }

    /**
     * Deep comparison between two protobuf {@link Message entities}, including nested entities. The two messages *must*
     * be of the same type (i.e.: {@code getDescriptorForType()} must return the same {@code Descriptor} for both).
     *
     * @return an @{@link Optional} {@link String} report of the differences, {@link Optional#empty()} if none are found.
     * @throws IllegalArgumentException when both messages are not of the same type
     * @throws NullPointerException     when any of the messages are <tt>null</tt>
     */
    public static <T extends Message> Optional<String> diffReport(T one, T other) {
        ErrorCollector collector = new ErrorCollector();
        new TestVerb(collector)
                .about(ProtoTruth.protos())
                .that(other)
                .named(one.getDescriptorForType().getName())
                .reportingMismatchesOnly()
                .isEqualTo(one);
        return collector.getFailure();
    }

    private static class ErrorCollector extends AbstractFailureStrategy {
        private volatile Optional<String> failure = Optional.empty();

        public Optional<String> getFailure() {
            return failure;
        }

        @Override
        public void fail(String message, Throwable cause) {
            this.failure = Optional.of(message);
        }
    }
}
