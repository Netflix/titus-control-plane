/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Given set of field names, creates a copy of protobuf object, with only the indicated fields included.
 */
public final class ProtobufCopy {

    private ProtobufCopy() {
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
                            if (!collection.isEmpty() && collection.iterator().next() instanceof Message) {
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

        return (T) builder.build();
    }
}
