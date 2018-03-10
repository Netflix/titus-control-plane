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

package io.netflix.titus.master.integration.v3.agent;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentInstanceGroups;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceBlockingStub;
import io.netflix.titus.master.integration.BaseIntegrationTest;
import io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStacks;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class AgentQueryTest extends BaseIntegrationTest {

    @ClassRule
    public static final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusStacks.basicStack(1));

    private static AgentManagementServiceBlockingStub client;

    @BeforeClass
    public static void setUp() throws Exception {
        client = titusStackResource.getGateway().getV3BlockingGrpcAgentClient();
    }

    @Test
    public void testGetAllServerGroups() throws Exception {
        AgentInstanceGroups serverGroups = client.getInstanceGroups(Empty.getDefaultInstance());
    }
}
