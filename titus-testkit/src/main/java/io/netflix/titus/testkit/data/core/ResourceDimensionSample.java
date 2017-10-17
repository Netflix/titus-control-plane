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

package io.netflix.titus.testkit.data.core;

import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.ResourceDimension.Builder;

public enum ResourceDimensionSample {

    Small() {
        @Override
        public Builder builder() {
            return ResourceDimension.newBuilder()
                    .withCpus(1)
                    .withMemoryMB(1024)
                    .withDiskMB(100)
                    .withNetworkMbs(100);

        }
    },
    SmallX2() {
        @Override
        public Builder builder() {
            return ResourceDimension.newBuilder()
                    .withCpus(2)
                    .withMemoryMB(2048)
                    .withDiskMB(200)
                    .withNetworkMbs(200);

        }
    };

    public abstract Builder builder();

    public ResourceDimension build() {
        return builder().build();
    }
}
