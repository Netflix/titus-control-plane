/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.feature;

import org.junit.Test;

import static com.netflix.titus.common.util.feature.FeatureGuards.alwaysApproved;
import static com.netflix.titus.common.util.feature.FeatureGuards.alwaysDenied;
import static com.netflix.titus.common.util.feature.FeatureGuards.alwaysUndecided;
import static com.netflix.titus.common.util.feature.FeatureGuards.toPredicate;
import static org.assertj.core.api.Assertions.assertThat;

public class FeatureGuardsTest {

    @Test
    public void testPredicates() {
        assertThat(toPredicate().test("anything")).isFalse();

        assertThat(toPredicate(alwaysApproved()).test("anything")).isTrue();
        assertThat(toPredicate(alwaysDenied()).test("anything")).isFalse();
        assertThat(toPredicate(alwaysUndecided()).test("anything")).isFalse();

        assertThat(toPredicate(alwaysUndecided(), alwaysUndecided()).test("anything")).isFalse();
        assertThat(toPredicate(alwaysUndecided(), alwaysApproved()).test("anything")).isTrue();
        assertThat(toPredicate(alwaysUndecided(), alwaysDenied()).test("anything")).isFalse();
        assertThat(toPredicate(alwaysApproved(), alwaysDenied()).test("anything")).isTrue();
        assertThat(toPredicate(alwaysDenied(), alwaysApproved()).test("anything")).isFalse();
    }
}