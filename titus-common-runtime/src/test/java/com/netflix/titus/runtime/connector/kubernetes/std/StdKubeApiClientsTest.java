/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.std;

import java.util.UUID;

import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiClients;
import okhttp3.Request;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StdKubeApiClientsTest {

    @Test
    public void testMapUri() {
        String uuid = UUID.randomUUID().toString();
        String instanceId = "i-07d1b67286b43458e";
        String path = "/segment1_" + uuid + "/segment2_" + instanceId;
        assertThat(StdKubeApiClients.mapUri(newRequest(path))).isEqualTo("/segment1_/segment2_");
    }

    private Request newRequest(String path) {
        return new Request.Builder().url("http://myservice" + path).build();
    }
}