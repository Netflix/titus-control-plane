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

package com.netflix.titus.ext.eureka.supervisor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.titus.api.supervisor.model.ReadinessState;
import com.netflix.titus.api.supervisor.model.ReadinessStatus;
import com.netflix.titus.api.supervisor.service.LocalMasterReadinessResolver;
import com.netflix.titus.common.runtime.TitusRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Singleton
public class EurekaLocalMasterReadinessResolver implements LocalMasterReadinessResolver {

    private static final Logger logger = LoggerFactory.getLogger(EurekaLocalMasterReadinessResolver.class);

    private final EurekaClient eurekaClient;
    private final String instanceId;
    private final TitusRuntime titusRuntime;

    @Inject
    public EurekaLocalMasterReadinessResolver(EurekaClient eurekaClient, EurekaSupervisorConfiguration configuration, TitusRuntime titusRuntime) {
        this.eurekaClient = eurekaClient;
        this.instanceId = configuration.getInstanceId();
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Flux<ReadinessStatus> observeLocalMasterReadinessUpdates() {
        return Flux.create(emitter -> {
            AtomicReference<ReadinessStatus> last = new AtomicReference<>(fetchCurrent());

            // Emit immediately known state
            emitter.next(last.get());

            EurekaEventListener listener = event -> {
                try {
                    ReadinessStatus next = fetchCurrent();
                    emitter.next(next);
                    last.set(next);
                } catch (Exception e) {
                    titusRuntime.getCodeInvariants().unexpectedError(
                            "EurekaClient event processing error: event=%s, error=%s", event, e.getMessage()
                    );
                    logger.debug("Unexpected failure", e);
                }
            };

            // There is a delay between the first fetchCurrent() operation, and the listener registration,  in which
            // case it is possible to miss the first notification. It is ok, as the notifications are delivered at
            // a regular interval by Eureka client.
            eurekaClient.registerEventListener(listener);
            emitter.onCancel(() -> eurekaClient.unregisterEventListener(listener));
        }, FluxSink.OverflowStrategy.LATEST);
    }

    private ReadinessStatus fetchCurrent() {
        List<InstanceInfo> instances = eurekaClient.getInstancesById(instanceId);

        if (instances.isEmpty()) {
            return ReadinessStatus.newBuilder()
                    .withState(ReadinessState.NotReady)
                    .withMessage("TitusMaster not registered with Eureka")
                    .withTimestamp(titusRuntime.getClock().wallTime())
                    .build();
        }

        InstanceInfo instance = instances.get(0);

        ReadinessStatus.Builder statusBuilder = ReadinessStatus.newBuilder().withTimestamp(titusRuntime.getClock().wallTime());

        switch (instance.getStatus()) {
            case STARTING:
                statusBuilder
                        .withState(ReadinessState.NotReady)
                        .withMessage("TitusMaster is not started yet");
                break;
            case UP:
                statusBuilder
                        .withState(ReadinessState.Enabled)
                        .withMessage("TitusMaster is UP in Eureka");
                break;
            case DOWN:
                statusBuilder
                        .withState(ReadinessState.Disabled)
                        .withMessage("TitusMaster is DOWN in Eureka");
                break;
            case OUT_OF_SERVICE:
                statusBuilder
                        .withState(ReadinessState.Disabled)
                        .withMessage("TitusMaster is OUT_OF_SERVICE in Eureka");
                break;
            case UNKNOWN:
            default:
                statusBuilder
                        .withState(ReadinessState.Disabled)
                        .withMessage("TitusMaster status is unknown by Eureka");
                break;
        }

        return statusBuilder.build();
    }
}
