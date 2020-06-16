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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KubeApiServerIntegratorTest {

    @Test
    public void testHasRequiredNodeAttributes() {
        List<Protos.Attribute> attributes = new ArrayList<>();
        assertThat(KubeApiServerIntegrator.hasRequiredNodeAttributes(attributes)).isFalse();

        attributes.add(Protos.Attribute.newBuilder().setName(KubeApiServerIntegrator.NODE_ATTRIBUTE_ID).setType(Protos.Value.Type.TEXT).build());
        attributes.add(Protos.Attribute.newBuilder().setName(KubeApiServerIntegrator.NODE_ATTRIBUTE_HOST_IP).setType(Protos.Value.Type.TEXT).build());
        attributes.add(Protos.Attribute.newBuilder().setName(KubeApiServerIntegrator.NODE_ATTRIBUTE_RES).setType(Protos.Value.Type.TEXT).build());
        assertThat(KubeApiServerIntegrator.hasRequiredNodeAttributes(attributes)).isTrue();
    }
}