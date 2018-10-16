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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Emitter;
import rx.Observable;

@Singleton
public class EurekaLocalMasterInstanceResolver implements LocalMasterInstanceResolver {

    private static final Logger logger = LoggerFactory.getLogger(EurekaLocalMasterInstanceResolver.class);

    private final EurekaClient eurekaClient;
    private final String instanceId;
    private final TitusRuntime titusRuntime;

    @Inject
    public EurekaLocalMasterInstanceResolver(EurekaClient eurekaClient, EurekaSupervisorConfiguration configuration, TitusRuntime titusRuntime) {
        this.eurekaClient = eurekaClient;
        this.instanceId = configuration.getInstanceId();
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Observable<MasterInstance> observeLocalMasterInstanceUpdates() {
        return Observable.create(emitter -> {
            AtomicReference<MasterInstance> last = new AtomicReference<>(fetchCurrent());

            // Emit immediately known state
            emitter.onNext(last.get());

            EurekaEventListener listener = event -> {
                try {
                    MasterInstance next = fetchCurrent();
                    if (MasterInstanceFunctions.areDifferent(next, last.get())) {
                        emitter.onNext(next);
                        last.set(next);
                    }
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
            emitter.setCancellation(() -> eurekaClient.unregisterEventListener(listener));
        }, Emitter.BackpressureMode.LATEST);
    }

    private MasterInstance fetchCurrent() {
        List<InstanceInfo> instances = eurekaClient.getInstancesById(instanceId);

        if (instances.isEmpty()) {
            return MasterInstance.newBuilder()
                    .withInstanceId(instanceId)
                    .withIpAddress("unknown")
                    .withStatus(MasterStatus.newBuilder()
                            .withState(MasterState.Inactive)
                            .withReasonCode(MasterStatus.REASON_CODE_UNHEALTHY)
                            .withReasonMessage("TitusMaster not registered with Eureka")
                            .withTimestamp(titusRuntime.getClock().wallTime())
                            .build())
                    .withStatusHistory(Collections.emptyList())
                    .build();
        }

        InstanceInfo instance = instances.get(0);

        MasterInstance.Builder builder = MasterInstance.newBuilder()
                .withInstanceId(instanceId)
                .withIpAddress(instance.getIPAddr())
                .withStatusHistory(Collections.emptyList());

        MasterStatus.Builder statusBuilder = MasterStatus.newBuilder()
                .withTimestamp(titusRuntime.getClock().wallTime());

        switch (instance.getStatus()) {
            case STARTING:
                statusBuilder
                        .withState(MasterState.Starting)
                        .withReasonCode(MasterStatus.REASON_CODE_NORMAL)
                        .withReasonMessage("TitusMaster is not started yet");
                break;
            case UP:
                statusBuilder
                        .withState(MasterState.NonLeader)
                        .withReasonCode(MasterStatus.REASON_CODE_NORMAL)
                        .withReasonMessage("TitusMaster is UP in Eureka");
                break;
            case DOWN:
                statusBuilder
                        .withState(MasterState.Inactive)
                        .withReasonCode(MasterStatus.REASON_CODE_UNHEALTHY)
                        .withReasonMessage("TitusMaster is DOWN in Eureka");
                break;
            case OUT_OF_SERVICE:
                statusBuilder
                        .withState(MasterState.Inactive)
                        .withReasonCode(MasterStatus.REASON_CODE_OUT_OF_SERVICE)
                        .withReasonMessage("TitusMaster is OUT_OF_SERVICE in Eureka");
                break;
            case UNKNOWN:
            default:
                statusBuilder
                        .withState(MasterState.Inactive)
                        .withReasonCode(MasterStatus.REASON_CODE_UNHEALTHY)
                        .withReasonMessage("TitusMaster status is unknown by Eureka");
                break;
        }

        return builder.withStatus(statusBuilder.build()).build();
    }
}
