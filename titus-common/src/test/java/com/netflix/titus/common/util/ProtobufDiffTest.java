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

import java.util.Optional;

import com.google.protobuf.Message;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufDiffTest {
    @Test
    public void testNoDifferences() {
        Message one = ProtoMessageBuilder.newInner("value1", "value2");
        Message other = ProtoMessageBuilder.newInner("value1", "value2");
        assertThat(ProtobufExt.diffReport(one, other)).isNotPresent();

        Message composed1 = ProtoMessageBuilder.newOuter(one, 5, one, other);
        Message composed2 = ProtoMessageBuilder.newOuter(one, 5, one, other);
        assertThat(ProtobufExt.diffReport(composed1, composed2)).isNotPresent();
    }

    // TODO(fabio): re-enable with a proper diff-ing implementation
    @Ignore
    @Test
    public void testTopLevelDifferences() {
        Message one = ProtoMessageBuilder.newInner("value1", "value2");
        Message other = ProtoMessageBuilder.newInner("value3", "value2");
        Message another = ProtoMessageBuilder.newInner("value3", "value4");

        Optional<String> diffOneOther = ProtobufExt.diffReport(one, other);
        assertThat(diffOneOther).hasValueSatisfying(report -> assertThat(report)
                .contains("stringField1")
                .doesNotContain("stringField2")
        );

        Optional<String> diffOneAnother = ProtobufExt.diffReport(one, another);
        assertThat(diffOneAnother).hasValueSatisfying(report -> assertThat(report)
                .contains("stringField1")
                .contains("stringField2")
        );
    }

    // TODO(fabio): re-enable with a proper diff-ing implementation
    @Ignore
    @Test
    public void testNestedDifferences() {
        Message one = ProtoMessageBuilder.newInner("value1", "value2");
        Message other = ProtoMessageBuilder.newInner("value3", "value2");
        Message another = ProtoMessageBuilder.newInner("value3", "value4");

        Message composed1 = ProtoMessageBuilder.newOuter(one, 5, one, other);
        Message composed2 = ProtoMessageBuilder.newOuter(one, 4, one, other);
        Message composed3 = ProtoMessageBuilder.newOuter(other, 5, one, other);
        Message composed4 = ProtoMessageBuilder.newOuter(other, 3, one, another);
        Message composed5 = ProtoMessageBuilder.newOuter(other, 5);

        assertThat(ProtobufExt.diffReport(composed1, composed2))
                .hasValueSatisfying(report -> assertThat(report)
                        .contains("primitiveField")
                        .doesNotContain("objectField")
                );

        assertThat(ProtobufExt.diffReport(composed1, composed3))
                .hasValueSatisfying(report -> assertThat(report)
                        .contains("objectField.stringField1")
                        .doesNotContain("objectField.stringField2")
                );

        assertThat(ProtobufExt.diffReport(composed1, composed4))
                .hasValueSatisfying(report -> assertThat(report)
                        .contains("primitiveField")
                        .contains("objectField.stringField1")
                        .doesNotContain("objectField.stringField2")
                        .doesNotContain("objectArrayField[0]")
                        .doesNotContain("objectArrayField[1].stringField1")
                        .contains("objectArrayField[1].stringField2")
                );

        assertThat(ProtobufExt.diffReport(composed1, composed5))
                .hasValueSatisfying(report -> assertThat(report)
                        .doesNotContain("primitiveField")
                        .contains("objectField.stringField1")
                        .doesNotContain("objectField.stringField2")
                        .contains("objectArrayField[0]")
                        .contains("objectArrayField[1]")
                );
    }
}
