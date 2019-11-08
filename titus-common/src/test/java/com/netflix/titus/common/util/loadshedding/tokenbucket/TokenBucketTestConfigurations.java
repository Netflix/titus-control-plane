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

package com.netflix.titus.common.util.loadshedding.tokenbucket;

import java.util.Map;

import com.netflix.titus.common.util.CollectionsExt;

class TokenBucketTestConfigurations {

    static Map<String, String> NOT_SHARED_PROPERTIES = CollectionsExt.asMap(
            "notShared.order", "10",
            "notShared.shared", "false",
            "notShared.callerPattern", "myUser.*",
            "notShared.endpointPattern", ".*",
            "notShared.capacity", "10",
            "notShared.refillRateInSec", "2"
    );

    static TokenBucketConfiguration NOT_SHARED_CONFIGURATION = new TokenBucketConfiguration(
            "notShared",
            10,
            false,
            "myUser.*",
            ".*",
            10,
            2
    );

    static Map<String, String> NOT_SHARED_BAD_PROPERTIES = CollectionsExt.asMap("notShared.order", "abc");

    static Map<String, String> DEFAULT_SHARED_PROPERTIES = CollectionsExt.asMap(
            "default.order", "100",
            "default.shared", "true",
            "default.callerPattern", ".*",
            "default.endpointPattern", ".*",
            "default.capacity", "100",
            "default.refillRateInSec", "20"
    );

    static TokenBucketConfiguration DEFAULT_SHARED_CONFIGURATION = new TokenBucketConfiguration(
            "default",
            100,
            true,
            ".*",
            ".*",
            100,
            20
    );

    static Map<String, String> DEFAULT_SHARED_BAD_PROPERTIES = CollectionsExt.asMap(
            "default.order", "100"
    );
}
