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

package io.netflix.titus.testkit.embedded.cloud.resource;

import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.testkit.model.PrimitiveValueGenerators;

public class ComputeResources {

    private volatile DataGenerator<String> ipsGenerator = PrimitiveValueGenerators.ipv4CIDRs("10.0.0.0/24");

    public String allocateIpAddress() {
        String address;
        synchronized (ipsGenerator) {
            address = ipsGenerator.getValue();
            this.ipsGenerator = ipsGenerator.apply();
        }
        return address;
    }

    public void releaseIpAddress(String ipAddress) {
        // No-op (in the future we should support IP address re-use).
    }

    public static String asHostname(String ipAddress, String instanceGroupName) {
        return ipAddress.replace('.', '_') + '.' + instanceGroupName +  ".titus.dev";
    }
}
