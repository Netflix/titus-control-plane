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

package com.netflix.titus.master.endpoint.v2.rest;

import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asMap;
import static com.netflix.titus.master.endpoint.v2.rest.QueryParametersUtil.buildLabelMapAndAppend;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class QueryParametersUtilTest {

    @Test
    public void testLabelParser() throws Exception {
        // Single label values
        testLabelParsing("labelA", asMap("labelA", null));
        testLabelParsing("labelA=", asMap("labelA", null));
        testLabelParsing("labelA=  ", asMap("labelA", null));
        testLabelParsing("labelA=valueA", asMap("labelA", "valueA"));
        testLabelParsing(" labelA = valueA", asMap("labelA", "valueA"));

        // Multi label values
        testLabelParsing("labelA,labelB", asMap("labelA", null, "labelB", null));
        testLabelParsing("labelA=,labelB=", asMap("labelA", null, "labelB", null));
        testLabelParsing("labelA=  , labelB=  ", asMap("labelA", null, "labelB", null));
        testLabelParsing("labelA=valueA,labelB=valueB", asMap("labelA", "valueA", "labelB", "valueB"));
        testLabelParsing(" labelA = valueA , labelB = valueB", asMap("labelA", "valueA", "labelB", "valueB"));
    }

    private void testLabelParsing(String labelA, Map<String, String> expectedResult) {
        Map<String, String> actualResult = new TreeMap<>();
        buildLabelMapAndAppend(labelA, actualResult);
        assertThat(actualResult, is(equalTo(expectedResult)));
    }
}