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
import java.util.Collections;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class ProtobufCopyTest {

    private static Descriptor INNER_TYPE;
    private static Descriptor OUTER_TYPE;

    private static FieldDescriptor INNER_FIELD_1;
    private static FieldDescriptor INNER_FIELD_2;
    private static FieldDescriptor OUTER_FIELD_1;
    private static FieldDescriptor OUTER_FIELD_2;

    private static DynamicMessage OUTER_VALUE;

    @BeforeClass
    public static void setUp() throws Exception {
        buildSampleProtoModel();
        buildSampleValues();
    }

    /**
     * Programmatically constructs test protobuf model:
     */
    private static void buildSampleProtoModel() throws Descriptors.DescriptorValidationException {
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
                .build();

        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("sampleProtoModel")
                .addMessageType(innerBuilder)
                .addMessageType(outerBuilder)
                .build();

        FileDescriptor fileDescriptor = FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
        INNER_TYPE = fileDescriptor.getMessageTypes().get(0);
        OUTER_TYPE = fileDescriptor.getMessageTypes().get(1);
    }

    private static void buildSampleValues() {
        INNER_FIELD_1 = INNER_TYPE.getFields().get(0);
        INNER_FIELD_2 = INNER_TYPE.getFields().get(1);
        DynamicMessage innerValue = DynamicMessage.newBuilder(INNER_TYPE)
                .setField(INNER_FIELD_1, "innerValue1")
                .setField(INNER_FIELD_2, "innerValue2")
                .build();

        OUTER_FIELD_1 = OUTER_TYPE.getFields().get(0);
        OUTER_FIELD_2 = OUTER_TYPE.getFields().get(1);
        OUTER_VALUE = DynamicMessage.newBuilder(OUTER_TYPE)
                .setField(OUTER_FIELD_1, innerValue)
                .setField(OUTER_FIELD_2, Collections.singletonList(innerValue))
                .build();
    }

    @Test
    public void testTopLevelFieldSelection() throws Exception {
        // Include all fields
        DynamicMessage all = ProtobufCopy.copy(OUTER_VALUE, asSet("objectField", "primitiveField"));
        assertFieldHasValue(all, OUTER_FIELD_1);
        assertFieldHasValue(all, OUTER_FIELD_2);

        // Include only second field
        DynamicMessage secondOnly = ProtobufCopy.copy(OUTER_VALUE, asSet("primitiveField"));
        assertFieldHasNoValue(secondOnly, OUTER_FIELD_1);
        assertFieldHasValue(secondOnly, OUTER_FIELD_2);
    }

    @Test
    public void testNestedSimpleFieldSelection() throws Exception {
        DynamicMessage filtered = ProtobufCopy.copy(OUTER_VALUE, asSet("objectField.stringField1", "primitiveField"));

        assertFieldHasValue(filtered, OUTER_FIELD_1);
        assertFieldHasValue((DynamicMessage) filtered.getField(OUTER_FIELD_1), INNER_FIELD_1);
        assertFieldHasNoValue((DynamicMessage) filtered.getField(OUTER_FIELD_1), INNER_FIELD_2);

        assertFieldHasValue(filtered, OUTER_FIELD_2);
    }

    @Test
    public void testNestedCollectionFieldSelection() throws Exception {
        DynamicMessage filtered = ProtobufCopy.copy(OUTER_VALUE, asSet("objectArrayField.stringField1", "primitiveField"));

        assertFieldHasNoValue(filtered, OUTER_FIELD_1);
        assertFieldHasValue(filtered, OUTER_FIELD_2);

        Collection<DynamicMessage> collection = (Collection<DynamicMessage>) filtered.getField(OUTER_FIELD_2);
        for (DynamicMessage dm : collection) {
            assertFieldHasValue(dm, INNER_FIELD_1);
            assertFieldHasNoValue(dm, INNER_FIELD_2);
        }
    }

    private void assertFieldHasValue(DynamicMessage entity, FieldDescriptor field) {
        Object value = entity.getField(field);
        assertThat(value).isNotNull();

        if (value instanceof DynamicMessage) {
            assertThat(((DynamicMessage) value).getAllFields()).isNotEmpty();
        } else {
            assertThat(value).isNotNull();
        }
    }

    private void assertFieldHasNoValue(DynamicMessage entity, FieldDescriptor field) {
        Object value = entity.getField(field);
        if (value != null) {
            if (value instanceof DynamicMessage) {
                assertThat(((DynamicMessage) value).getAllFields()).isEmpty();
            } else if (value instanceof String) {
                assertThat(value).isEqualTo("");
            } else {
                fail("Expected null value for field " + field);
            }
        }
    }
}