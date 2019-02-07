/*
 * Copyright 2019 Netflix, Inc.
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

import java.util.Arrays;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoMessageBuilder {
    private static final Descriptors.Descriptor INNER_TYPE;
    private static final Descriptors.Descriptor OUTER_TYPE;

    static {
        // Inner type
        FieldDescriptorProto innerField1 = FieldDescriptorProto.newBuilder()
                .setName("stringField1")
                .setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .build();
        FieldDescriptorProto innerField2 = FieldDescriptorProto.newBuilder()
                .setName("stringField2")
                .setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .build();

        DescriptorProtos.DescriptorProto innerBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("InnerEntity")
                .addField(innerField1)
                .addField(innerField2)
                .build();

        // Outer type
        FieldDescriptorProto outerField1 = FieldDescriptorProto.newBuilder()
                .setName("objectField")
                .setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("InnerEntity")
                .build();
        FieldDescriptorProto outerField2 = FieldDescriptorProto.newBuilder()
                .setName("primitiveField")
                .setNumber(3)
                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                .build();
        FieldDescriptorProto outerField3 = FieldDescriptorProto.newBuilder()
                .setName("objectArrayField")
                .setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("InnerEntity")
                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                .build();

        DescriptorProtos.DescriptorProto outerBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OuterEntity")
                .addField(outerField1)
                .addField(outerField2)
                .addField(outerField3)
                .build();

        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("sampleProtoModel")
                .addMessageType(innerBuilder)
                .addMessageType(outerBuilder)
                .build();

        FileDescriptor fileDescriptor = null;
        try {
            fileDescriptor = FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
        INNER_TYPE = fileDescriptor.getMessageTypes().get(0);
        OUTER_TYPE = fileDescriptor.getMessageTypes().get(1);
    }

    static Message newInner(String value1, String value2) {
        FieldDescriptor field1 = INNER_TYPE.getFields().get(0);
        FieldDescriptor field2 = INNER_TYPE.getFields().get(1);
        return DynamicMessage.newBuilder(INNER_TYPE)
                .setField(field1, value1)
                .setField(field2, value2)
                .build();
    }

    static Message newOuter(Message value1, int value2, Message... value3) {
        FieldDescriptor field1 = OUTER_TYPE.getFields().get(0);
        FieldDescriptor field2 = OUTER_TYPE.getFields().get(1);
        FieldDescriptor field3 = OUTER_TYPE.getFields().get(2);
        return DynamicMessage.newBuilder(OUTER_TYPE)
                .setField(field1, value1)
                .setField(field2, value2)
                .setField(field3, Arrays.asList(value3))
                .build();
    }

    static FieldDescriptor getAndAssertField(Descriptors.Descriptor descriptor, String name) {
        FieldDescriptor field = descriptor.findFieldByName(name);
        assertThat(field).isNotNull();
        return field;
    }

    static FieldDescriptor getAndAssertField(Message message, String name) {
        return getAndAssertField(message.getDescriptorForType(), name);
    }
}
