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

package com.netflix.titus.master.kubernetes.controller;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.kubernetes.client.DirectKubeConfiguration;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import com.netflix.titus.master.kubernetes.client.model.PodNotFoundEvent;
import com.netflix.titus.master.kubernetes.KubernetesConfiguration;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Singleton
public class DefaultKubeJobManagementReconciler implements KubeJobManagementReconciler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKubeJobManagementReconciler.class);

    static final String GC_UNKNOWN_PODS = "gcUnknownPods";

    private enum OrphanedKind {
        /**
         * If the last task status was KillInitiated and the system missed the last event then the assumption is that
         * the kubelet successfully terminated the pod and deleted the pod object.
         */
        KILL_INITIATED,

        /**
         * If task is associated with an non-existing agent, assume the agent was terminated.
         */
        NODE_TERMINATED,

        UNKNOWN
    }

    private final KubernetesConfiguration kubernetesConfiguration;
    private final DirectKubeConfiguration directKubeConfiguration;
    private final FixedIntervalTokenBucketConfiguration gcUnknownPodsTokenBucketConfiguration;
    private final StdKubeApiFacade kubeApiFacade;
    private final V3JobOperations v3JobOperations;

    private final Clock clock;
    private final TitusRuntime titusRuntime;

    private final DirectProcessor<PodEvent> podEventProcessor = DirectProcessor.create();
    private final FluxSink<PodEvent> podEventSink = podEventProcessor.sink(FluxSink.OverflowStrategy.IGNORE);

    private final Map<OrphanedKind, Gauge> orphanedTaskGauges;

    private ScheduleReference schedulerRef;

    @Inject
    public DefaultKubeJobManagementReconciler(KubernetesConfiguration kubernetesConfiguration,
                                              DirectKubeConfiguration directKubeConfiguration,
                                              @Named(GC_UNKNOWN_PODS) FixedIntervalTokenBucketConfiguration gcUnknownPodsTokenBucketConfiguration,
                                              StdKubeApiFacade kubeApiFacade,
                                              V3JobOperations v3JobOperations,
                                              TitusRuntime titusRuntime) {
        this.kubernetesConfiguration = kubernetesConfiguration;
        this.directKubeConfiguration = directKubeConfiguration;
        this.gcUnknownPodsTokenBucketConfiguration = gcUnknownPodsTokenBucketConfiguration;
        this.kubeApiFacade = kubeApiFacade;
        this.v3JobOperations = v3JobOperations;
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;

        Registry registry = titusRuntime.getRegistry();

        this.orphanedTaskGauges = Stream.of(OrphanedKind.values()).collect(Collectors.toMap(
                Function.identity(),
                kind -> registry.gauge(MetricConstants.METRIC_KUBERNETES + "orphanedTasks", "kind", kind.name())
        ));
    }

    @Activator
    public void enterActiveMode() {
        ScheduleDescriptor scheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("reconcileNodesAndPods")
                .withDescription("Reconcile nodes and pods")
                .withInitialDelay(Duration.ofMillis(kubernetesConfiguration.getReconcilerInitialDelayMs()))
                .withInterval(Duration.ofMillis(kubernetesConfiguration.getReconcilerIntervalMs()))
                .withTimeout(Duration.ofMinutes(5))
                .build();

        this.schedulerRef = titusRuntime.getLocalScheduler().schedule(
                scheduleDescriptor,
                e -> reconcile(),
                ExecutorsExt.namedSingleThreadExecutor(DefaultKubeJobManagementReconciler.class.getSimpleName())
        );
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

    @Override
    public Flux<PodEvent> getPodEventSource() {
        return podEventProcessor.transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    private void reconcile() {
        if (!kubernetesConfiguration.isReconcilerEnabled()) {
            logger.info("Skipping the job management / Kube reconciliation cycle: reconciler disabled");
            return;
        }
        if (!kubeApiFacade.getNodeInformer().hasSynced() || !kubeApiFacade.getPodInformer().hasSynced()) {
            logger.info("Skipping the job management / Kube reconciliation cycle: Kube informers not ready (node={}, pod={})",
                    kubeApiFacade.getNodeInformer().hasSynced(), kubeApiFacade.getPodInformer().hasSynced()
            );
            return;
        }

        List<V1Node> nodes = kubeApiFacade.getNodeInformer().getIndexer().list()
                .stream()
                .filter(n -> StringExt.isNotEmpty(KubeUtil.getMetadataName(n.getMetadata())))
                .collect(Collectors.toList());
        List<V1Pod> pods = kubeApiFacade.getPodInformer().getIndexer().list()
                .stream()
                .filter(p -> StringExt.isNotEmpty(KubeUtil.getMetadataName(p.getMetadata())))
                .collect(Collectors.toList());
        List<Task> tasks = v3JobOperations.getTasks();

        Map<String, V1Node> nodesById = nodes.stream().collect(Collectors.toMap(
                node -> KubeUtil.getMetadataName(node.getMetadata()),
                Function.identity()
        ));
        Map<String, Task> currentTasks = tasks.stream().collect(Collectors.toMap(Task::getId, Function.identity()));
        Set<String> currentPodNames = pods.stream().map(p -> KubeUtil.getMetadataName(p.getMetadata())).collect(Collectors.toSet());

        transitionOrphanedTasks(currentTasks, currentPodNames, nodesById);
    }

    /**
     * Transition orphaned tasks to Finished that don't exist in Kubernetes.
     */
    private void transitionOrphanedTasks(Map<String, Task> currentTasks, Set<String> currentPodNames, Map<String, V1Node> nodes) {
        List<Task> tasksNotInApiServer = currentTasks.values().stream()
                .filter(t -> shouldTaskBeInApiServer(t) && !currentPodNames.contains(t.getId()))
                .collect(Collectors.toList());

        Map<OrphanedKind, List<Task>> orphanedTasksByKind = new HashMap<>();
        for (Task task : tasksNotInApiServer) {
            if (task.getStatus().getState().equals(TaskState.KillInitiated)) {
                orphanedTasksByKind.computeIfAbsent(OrphanedKind.KILL_INITIATED, s -> new ArrayList<>()).add(task);
            } else {
                if (findNode(task, nodes).isPresent()) {
                    orphanedTasksByKind.computeIfAbsent(OrphanedKind.UNKNOWN, s -> new ArrayList<>()).add(task);
                } else {
                    orphanedTasksByKind.computeIfAbsent(OrphanedKind.NODE_TERMINATED, s -> new ArrayList<>()).add(task);
                }
            }
        }

        orphanedTasksByKind.forEach((kind, tasks) -> {
            logger.info("Attempting to transition {} orphaned tasks to finished ({}): {}", tasks.size(), kind, tasks);
            orphanedTaskGauges.get(kind).set(tasks.size());

            for (Task task : tasks) {

                String reasonCode;
                String reasonMessage;
                switch (kind) {
                    case KILL_INITIATED:
                        reasonCode = TaskStatus.REASON_TASK_KILLED;
                        reasonMessage = "Task killed";
                        break;
                    case NODE_TERMINATED:
                        reasonCode = TaskStatus.REASON_TASK_LOST;
                        reasonMessage = "Terminated due to an issue with the underlying host machine";
                        break;
                    case UNKNOWN:
                    default:
                        reasonCode = TaskStatus.REASON_TASK_LOST;
                        reasonMessage = "Abandoned with unknown state due to lack of status updates from the host machine";
                        break;
                }

                publishEvent(task,
                        TaskStatus.newBuilder()
                                .withState(TaskState.Finished)
                                .withReasonCode(reasonCode)
                                .withReasonMessage(reasonMessage)
                                .withTimestamp(clock.wallTime())
                                .build()
                );
            }
            logger.info("Finished orphaned task transitions to finished ({})", kind);
        });
    }

    private Optional<V1Node> findNode(Task task, Map<String, V1Node> nodes) {
        // Node name may be different from agent instance id. We use the instance id attribute only as a fallback.
        String nodeName = task.getTaskContext().getOrDefault(
                TaskAttributes.TASK_ATTRIBUTES_KUBE_NODE_NAME,
                task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID)
        );
        if (nodeName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(nodes.get(nodeName));
    }

    private boolean shouldTaskBeInApiServer(Task task) {
        boolean isRunning = TaskState.isRunning(task.getStatus().getState());
        if (isRunning) {
            return true;
        }
        if (task.getStatus().getState() == TaskState.Accepted && TaskStatus.hasPod(task)) {
            return clock.isPast(task.getStatus().getTimestamp() + kubernetesConfiguration.getOrphanedPodTimeoutMs());
        }
        return false;
    }

    private void publishEvent(Task task, TaskStatus finalTaskStatus) {
        publishPodEvent(task, finalTaskStatus);
    }

    private void publishPodEvent(Task task, TaskStatus finalTaskStatus) {
        PodNotFoundEvent podEvent = PodEvent.onPodNotFound(task, finalTaskStatus);
        logger.debug("Publishing pod event: {}", podEvent);
        podEventSink.next(podEvent);
    }
}
