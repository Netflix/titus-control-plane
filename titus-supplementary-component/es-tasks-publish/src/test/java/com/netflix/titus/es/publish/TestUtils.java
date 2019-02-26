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
package com.netflix.titus.es.publish;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.fail;

public class TestUtils {
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    public static class CountResponse {
        int count;

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }
    }

    static void runLocalDockerCmd(String dockerCmd) throws Exception {
        int exitCode = Runtime.getRuntime().exec(dockerCmd).waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Unable to launch local ES container");
        }
    }

    static String buildDockerRunCmd(String containerName) {
        return "docker run --rm --name " + containerName +
                " -d -p 9200:9200 -p 9300:9300 -e xpack.security.enabled=false -e discovery.type=single-node docker.elastic.co/elasticsearch/elasticsearch:5.6.15";
    }

    static String buildDockerStopCmd(String containerName) {
        return "docker stop " + containerName;
    }

    /*
    static ManagedChannel buildTitusGrpcChannel(TitusClientUtils titusClientUtils) {
        return NettyChannelBuilder.forAddress(titusClientUtils.buildTitusApiHost(), 7104)
                .userAgent("testClient")
                .sslContext(ClientAuthenticationUtil.newSslContext(TitusClientImpl.METATRON_APP_NAME))
                .negotiationType(NegotiationType.TLS)
                .build();
    }
    */


    static EsPublisherConfiguration buildEsPublisherConfiguration() {
        return new EsPublisherConfiguration(
                "us-east-1",
                "test",
                "test",
                "localhost",
                9200,
                "yyyyMM",
                "titustasks_",
                true
        );
    }

    static void verifyLocalEsRecordCount(EsClientHttp esClientHttp, int count) {
        final CountResponse countResponse = WebClient.create().get()
                .uri(String.format("http://localhost:9200/%s/_count", esClientHttp.buildEsIndexNameCurrent()))
                .retrieve().bodyToMono(CountResponse.class).block();
        assertThat(countResponse).isNotNull();
        assertThat(countResponse.count).isEqualTo(count);
    }

    static void waitForLocalElasticServerReadiness(int maxAttempts) throws InterruptedException {
        boolean isReady = false;
        int numAttempt = 0;
        while (!isReady) {
            try {
                checkLocalEsClusterHealth();
                isReady = true;
            } catch (Exception ex) {
                isReady = false;
                Thread.sleep(5000);
            }
            numAttempt = numAttempt + 1;
            if (numAttempt >= maxAttempts) {
                isReady = false;
                fail(String.format("%d attempts reached - ES server not ready.", maxAttempts));
            }
        }
    }

    static void checkLocalEsClusterHealth() {
        final ClientResponse response = WebClient.create().get().uri("http://localhost:9200/_cluster/health")
                .exchange().block(Duration.ofSeconds(30));
        if (response == null || !response.statusCode().is2xxSuccessful()) {
            throw new RuntimeException("Server not ready");
        }
    }

    static List<Task> generateSampleTasks(int numTasks) {
        return IntStream.range(0, numTasks)
                .mapToObj(ignored -> V3GrpcModelConverters.toGrpcTask(JobGenerator.oneBatchTask(), new EmptyLogStorageInfo<>()))
                .collect(Collectors.toList());
    }
}
