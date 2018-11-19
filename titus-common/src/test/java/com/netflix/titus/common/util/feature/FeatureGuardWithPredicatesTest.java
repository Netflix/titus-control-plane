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

package com.netflix.titus.common.util.feature;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FeatureGuardWithPredicatesTest {

    @Test
    public void testWhiteList() {
        Predicate<String> featureGuard = FeatureGuards.toPredicate(FeatureGuards.newWhiteList()
                .withTurnOnPredicate(() -> true)
                .withWhiteListRegExpSupplier("whiteList", () -> "enabled.*")
                .withBlackListRegExpSupplier("blackList", () -> "disabled.*")
                .build()
        );

        assertThat(featureGuard.test("enabledABC")).isTrue();
        assertThat(featureGuard.test("disabledABC")).isFalse();
        assertThat(featureGuard.test("unmatched")).isFalse();
    }

    @Test
    public void testDynamicTurnOff() {
        AtomicBoolean turnOnRef = new AtomicBoolean(true);

        Predicate<String> featureGuard = FeatureGuards.toPredicate(FeatureGuards.newWhiteList()
                .withTurnOnPredicate(turnOnRef::get)
                .withWhiteListRegExpSupplier("whiteList", () -> "enabled.*")
                .build()
        );

        assertThat(featureGuard.test("enabledABC")).isTrue();
        turnOnRef.set(false);
        assertThat(featureGuard.test("enabledABC")).isFalse();
    }

    @Test
    public void testDynamicWhiteListUpdate() {
        AtomicReference<String> whiteListRef = new AtomicReference<>("enabled.*");

        Predicate<String> featureGuard = FeatureGuards.toPredicate(FeatureGuards.newWhiteList()
                .withTurnOnPredicate(() -> true)
                .withWhiteListRegExpSupplier("whiteList", whiteListRef::get)
                .build()
        );

        assertThat(featureGuard.test("enabledABC")).isTrue();
        whiteListRef.set("enabledDEF");
        assertThat(featureGuard.test("enabledABC")).isFalse();
    }

    @Test
    public void testConfiguration() {
        FeatureGuardWhiteListConfiguration configuration = new FeatureGuardWhiteListConfiguration() {
            @Override
            public boolean isTurnedOn() {
                return true;
            }

            @Override
            public String getWhiteList() {
                return "enabled.*";
            }

            @Override
            public String getBlackList() {
                return "disabled.*";
            }
        };
        Predicate<String> featureGuard = FeatureGuards.toPredicate(FeatureGuards.newWhiteListFromConfiguration(configuration).build());

        assertThat(featureGuard.test("enabledABC")).isTrue();
        assertThat(featureGuard.test("disabledABC")).isFalse();
        assertThat(featureGuard.test("unmatched")).isFalse();
    }
}