/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.client;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.histogram.BucketCounter;
import com.netflix.spectator.api.histogram.BucketFunctions;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.kubernetes.KubeUtil;
import io.kubernetes.client.openapi.models.V1Pod;

/**
 * Metrics companion class for {@link DefaultDirectKubeApiServerIntegrator}.
 */
class DefaultDirectKubeApiServerIntegratorMetrics {

    private static final String ROOT = MetricConstants.METRIC_KUBERNETES + "directKubeApiServerIntegrator.";
    private static final String PV_ROOT = ROOT + "persistentVolume.";
    private static final String PVC_ROOT = ROOT + "persistentVolumeClaim.";

    private final Registry registry;

    private final Id podGaugeId;
    private final Id launchCounterId;
    private final Id terminateCounterId;
    private final Id eventCounterId;
    private final Id persistentVolumeCreateCounterId;
    private final Id persistentVolumeClaimCreateCounterId;

    private final BucketCounter podSizeMetrics;

    DefaultDirectKubeApiServerIntegratorMetrics(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.podGaugeId = registry.createId(ROOT + "pods");
        this.launchCounterId = registry.createId(ROOT + "launches");
        this.terminateCounterId = registry.createId(ROOT + "terminates");
        this.eventCounterId = registry.createId(ROOT + "events");
        this.persistentVolumeCreateCounterId = registry.createId(PV_ROOT + "create");
        this.persistentVolumeClaimCreateCounterId = registry.createId(PVC_ROOT + "create");

        this.podSizeMetrics = BucketCounter.get(
                registry,
                registry.createId(ROOT + "podSize"),
                BucketFunctions.bytes(32768)
        );
    }

    void shutdown() {
        PolledMeter.remove(registry, podGaugeId);
    }

    void observePodsCollection(ConcurrentMap<String, V1Pod> pods) {
        PolledMeter.using(registry).withId(podGaugeId).monitorSize(pods);
    }

    void persistentVolumeCreateSuccess(long elapsedMs) {
        registry.timer(persistentVolumeCreateCounterId.withTag("status", "success")).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void persistentVolumeCreateError(Throwable error, long elapsedMs) {
        registry.timer(persistentVolumeCreateCounterId.withTags(
                "status", "error",
                "error", error.getClass().getSimpleName()
        )).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void persistentVolumeClaimCreateSuccess(long elapsedMs) {
        registry.timer(persistentVolumeClaimCreateCounterId.withTag("status", "success")).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void persistentVolumeClaimCreateError(Throwable error, long elapsedMs) {
        registry.timer(persistentVolumeClaimCreateCounterId.withTags(
                "status", "error",
                "error", error.getClass().getSimpleName()
        )).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void launchSuccess(Task task, V1Pod v1Pod, long elapsedMs) {
        podSizeMetrics.record(KubeUtil.estimatePodSize(v1Pod));
        registry.timer(launchCounterId.withTag("status", "success")).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void launchError(Task task, Throwable error, long elapsedMs) {
        registry.timer(launchCounterId.withTags(
                "status", "error",
                "error", error.getClass().getSimpleName()
        )).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void launchTimeout(long elapsedMs) {
        registry.timer(launchCounterId.withTag("status", "timeout")).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void terminateSuccess(Task task, long elapsedMs) {
        registry.timer(terminateCounterId.withTag("status", "success")).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void terminateError(Task task, Throwable error, long elapsedMs) {
        registry.timer(terminateCounterId.withTags(
                "status", "error",
                "error", error.getClass().getSimpleName()
        )).record(elapsedMs, TimeUnit.MILLISECONDS);
    }

    void onAdd(V1Pod pod) {
        registry.counter(eventCounterId.withTags(
                "kind", "add"
        )).increment();
    }

    void onUpdate(V1Pod pod) {
        registry.counter(eventCounterId.withTags(
                "kind", "update"
        )).increment();
    }

    void onDelete(V1Pod pod) {
        registry.counter(eventCounterId.withTags(
                "kind", "delete"
        )).increment();
    }
}
