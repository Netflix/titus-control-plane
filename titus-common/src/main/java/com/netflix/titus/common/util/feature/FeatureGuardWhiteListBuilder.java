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

import java.util.function.Predicate;
import java.util.function.Supplier;

import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.feature.FeatureGuardWithPredicates.PredicateConfig;
import com.netflix.titus.common.util.feature.FeatureGuardWithPredicates.StepRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureGuardWhiteListBuilder {

    private static final Logger logger = LoggerFactory.getLogger(FeatureGuardWhiteListBuilder.class);

    private Supplier<Boolean> turnOnPredicate;

    private int regExpFlags;

    private String whiteListRegExpSupplierName;
    private Supplier<String> whiteListRegExpSupplier;

    private String blackListRegExpSupplierName;
    private Supplier<String> blackListRegExpSupplier;

    public FeatureGuardWhiteListBuilder withTurnOnPredicate(Supplier<Boolean> turnOnPredicate) {
        this.turnOnPredicate = turnOnPredicate;
        return this;
    }

    public FeatureGuardWhiteListBuilder withRegExpFlags(int regExpFlags) {
        this.regExpFlags = regExpFlags;
        return this;
    }

    public FeatureGuardWhiteListBuilder withWhiteListRegExpSupplier(String name, Supplier<String> supplier) {
        this.whiteListRegExpSupplierName = name;
        this.whiteListRegExpSupplier = supplier;
        return this;
    }

    public FeatureGuardWhiteListBuilder withBlackListRegExpSupplier(String name, Supplier<String> supplier) {
        this.blackListRegExpSupplierName = name;
        this.blackListRegExpSupplier = supplier;
        return this;
    }

    public FeatureGuard<String> build() {
        return new FeatureGuardWithPredicates(
                new PredicateConfig(StepRule.StopOnFalse, value -> turnOnPredicate.get(), false),
                new PredicateConfig(StepRule.StopOnTrue, newRegExpPredicate(whiteListRegExpSupplier, whiteListRegExpSupplierName), false),
                new PredicateConfig(StepRule.StopOnTrue, newRegExpPredicate(blackListRegExpSupplier, blackListRegExpSupplierName), true)
        );
    }

    private Predicate<String> newRegExpPredicate(Supplier<String> regExpSupplier, String regExpSupplierName) {
        Predicate<String> whiteListPredicate;
        if (regExpSupplier == null) {
            whiteListPredicate = value -> true;
        } else {
            whiteListPredicate = value ->
                    RegExpExt.dynamicMatcher(regExpSupplier, regExpSupplierName, regExpFlags, logger)
                            .apply(value)
                            .matches();
        }
        return whiteListPredicate;
    }
}
