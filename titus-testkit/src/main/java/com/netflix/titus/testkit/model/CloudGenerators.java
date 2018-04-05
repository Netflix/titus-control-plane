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

package com.netflix.titus.testkit.model;

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Cloud data generator (IPv4 addresses, host names, server group names, etc).
 */
public final class CloudGenerators {

    public static DataGenerator<String> vmIds() {
        return PrimitiveValueGenerators.hexValues(8).map(v -> "i-" + v);
    }

    public static DataGenerator<String> serverGroupIds() {
        return DataGenerator.range(1).map(idx -> "titusagent-" + idx);
    }

    public static DataGenerator<Pair<String, String>> ipAndHostNames() {
        return PrimitiveValueGenerators.ipv4CIDRs("10.0.0.0/8").map(ip ->
                Pair.of(ip, ip.replace('.', '_') + ".titus.dev")
        );
    }
}
