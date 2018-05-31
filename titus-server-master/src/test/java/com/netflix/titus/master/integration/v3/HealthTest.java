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

package com.netflix.titus.master.integration.v3;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.SERVING;
import static com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters.basicMaster;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class HealthTest extends BaseIntegrationTest {
    public static final TitusStackResource titusStackResource = new TitusStackResource(
            EmbeddedTitusCell.aTitusCell()
                    .withMaster(basicMaster(new SimulatedCloud()))
                    .withDefaultGateway()
                    .build()
    );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource);

    @Test(timeout = 30_000L)
    public void healthStatus() throws Exception {
        TestStreamObserver<HealthCheckResponse> testStreamObserver = new TestStreamObserver<>();
        titusStackResource.getOperations().getHealthClient().check(HealthCheckRequest.newBuilder().build(), testStreamObserver);
        HealthCheckResponse response = testStreamObserver.takeNext(10, TimeUnit.SECONDS);
        assertThat(testStreamObserver.hasError()).isFalse();
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(SERVING);
        assertThat(response.getDetailsCount()).isGreaterThan(0);
        assertThat(response.getDetails(0).hasDetails()).isTrue();
        HealthCheckResponse.Details details = response.getDetails(0).getDetails();
        assertThat(details.getStatus()).isEqualTo(SERVING);
        assertThat(details.hasUptime()).isTrue();
        assertThat(details.hasElectionTimestamp()).isTrue();
        assertThat(details.hasActivationTime()).isTrue();
        assertThat(details.hasActivationTimestamp()).isTrue();
    }
}
