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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asMap;
import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class PropertiesExtTest {

    @Test
    public void testSplitByRootName() throws Exception {
        Map<String, String> properties = asMap(
                "my.a", "valueA",
                "my.b", "valueB",
                "your.x", "valueX",
                "your.y", "valueY"
        );
        Map<String, Map<String, String>> groups = PropertiesExt.groupByRootName(properties, 1);

        assertThat(groups).hasSize(2);
        assertThat(groups.get("my")).containsAllEntriesOf(asMap("a", "valueA", "b", "valueB"));
        assertThat(groups.get("your")).containsAllEntriesOf(asMap("y", "valueX", "y", "valueY"));
    }

    @Test
    public void testGetPropertiesOf() throws Exception {
        Map<String, String> properties = asMap(
                "root.my.a", "valueA",
                "root.my.b", "valueB",
                "your.x", "valueX"
        );
        Map<String, String> subProps = PropertiesExt.getPropertiesOf("root.my", properties);
        assertThat(subProps).containsAllEntriesOf(asMap("a", "valueA", "b", "valueB"));
    }

    @Test
    public void testVerifyRootName() throws Exception {
        Map<String, String> properties = asMap(
                "my", "valueA",
                "your.x", "valueX",
                "their.y", "valueY"
        );
        List<String> notMatching = PropertiesExt.verifyRootNames(properties, asSet("my", "your"));
        assertThat(notMatching).containsExactly("their.y");
    }

    @Test
    public void testGetTopNames() {
        Set<String> result = PropertiesExt.getTopNames(asList("top1.a", "top2.b", "single"), 1);
        assertThat(result).contains("top1", "top2");
    }

    @Test
    public void testSplitNames() throws Exception {
        Map<String, Set<String>> result = PropertiesExt.splitNames(asList("top1", "top2.nested2"), 1);
        assertThat(result).containsEntry("top1", null);
        assertThat(result).containsEntry("top2", asSet("nested2"));
    }

    @Test
    public void testFullSplit() throws Exception {
        PropertiesExt.PropertyNode<Boolean> result = PropertiesExt.fullSplit(asList("top1", "top2.nested2"));
        assertThat(result.getChildren()).containsKeys("top1", "top2");
        assertThat(result.getChildren().get("top1").getValue()).contains(true);
        assertThat(result.getChildren().get("top2").getValue()).isEmpty();
        assertThat(result.getChildren().get("top2").getChildren()).containsKeys("nested2");
        assertThat(result.getChildren().get("top2").getChildren().get("nested2").getValue()).contains(true);
    }
}