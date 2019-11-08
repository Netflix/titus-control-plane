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
            "notShared.sharedByCallers", "false",
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
            5,
            1
    );

    static Map<String, String> NOT_SHARED_BAD_PROPERTIES = CollectionsExt.asMap("notShared.order", "abc");

    static Map<String, String> SHARED_ANY_PROPERTIES = CollectionsExt.asMap(
            "sharedAny.order", "100",
            "sharedAny.sharedByCallers", "true",
            "sharedAny.callerPattern", ".*",
            "sharedAny.endpointPattern", ".*",
            "sharedAny.capacity", "100",
            "sharedAny.refillRateInSec", "20"
    );

    static TokenBucketConfiguration SHARED_ANY_CONFIGURATION = new TokenBucketConfiguration(
            "sharedAny",
            100,
            true,
            ".*",
            ".*",
            10,
            2
    );

    static TokenBucketConfiguration SHARED_GETTERS_CONFIGURATION = new TokenBucketConfiguration(
            "sharedGetters",
            90,
            true,
            ".*",
            "get.*",
            10,
            2
    );

    static Map<String, String> SHARED_ANY_BAD_PROPERTIES = CollectionsExt.asMap(
            "sharedAny.order", "100"
    );
}
