package io.netflix.titus.master.jobmanager.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.DateTimeExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.histogram.Histogram;
import io.netflix.titus.common.util.histogram.HistogramDescriptor;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Subscription;
import rx.schedulers.Schedulers;

/**
 * Supplementary metrics based on both job/task state, and elapsed time. These metrics cannot be computed only
 * in response to system state change events. Instead, they are recomputed at regular interval.
 *
 * TODO V3 job manager integration
 */
@Singleton
public class TaskLivenessMetrics {

    private static final Logger logger = LoggerFactory.getLogger(TaskLivenessMetrics.class);

    private static final String ROOT_METRIC_NAME = MetricConstants.METRIC_ROOT + "jobManager.taskLiveness.";
    private static final String TASK_METRIC_NAME = ROOT_METRIC_NAME + "duration";

    private static final long REFRESH_INTERVAL_SEC = 30;

    private static final List<String> TRACKED_STATES = Arrays.asList(
            V2JobState.Accepted.name(),
            V2JobState.Launched.name(),
            V2JobState.StartInitiated.name(),
            V2JobState.Started.name()
    );

    private static final HistogramDescriptor HISTOGRAM_DESCRIPTOR = HistogramDescriptor.histogramOf(
            TimeUnit.MINUTES.toMillis(1),
            TimeUnit.MINUTES.toMillis(15),
            TimeUnit.MINUTES.toMillis(30),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(6),
            TimeUnit.HOURS.toMillis(12),
            TimeUnit.HOURS.toMillis(24),
            TimeUnit.DAYS.toMillis(2),
            TimeUnit.DAYS.toMillis(3),
            TimeUnit.DAYS.toMillis(4),
            TimeUnit.DAYS.toMillis(5)
    );

    private final ApplicationSlaManagementService applicationSlaManagementService;
    private final V2JobOperations v2JobOperations;
    private final Registry registry;

    private final Map<String, Map<String, List<Gauge>>> capacityGroupsMetrics = new HashMap<>();

    private Subscription subscription;

    @Inject
    public TaskLivenessMetrics(ApplicationSlaManagementService applicationSlaManagementService,
                               V2JobOperations v2JobOperations,
                               Registry registry) {
        this.applicationSlaManagementService = applicationSlaManagementService;
        this.v2JobOperations = v2JobOperations;
        this.registry = registry;
    }

    @Activator
    public void enterActiveMode() {
        this.subscription = ObservableExt.schedule(
                ROOT_METRIC_NAME + "scheduler", registry, "TaskLivenessRefreshAction",
                Completable.fromAction(this::refresh), REFRESH_INTERVAL_SEC, REFRESH_INTERVAL_SEC, TimeUnit.SECONDS, Schedulers.computation()
        ).subscribe(result -> {
            result.ifPresent(error -> logger.warn("Task liveness metrics refresh error", error));
        });
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(subscription);
    }

    private void refresh() {
        Map<String, Tier> tierMap = buildTierMap();
        Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms = buildCapacityGroupsHistograms(tierMap.keySet());
        resetDroppedCapacityGroups(capacityGroupsHistograms.keySet());
        updateCapacityGroupCounters(capacityGroupsHistograms, tierMap);
    }

    private void resetDroppedCapacityGroups(Set<String> knownCapacityGroups) {
        CollectionsExt.copyAndRemove(capacityGroupsMetrics.keySet(), knownCapacityGroups).forEach(absent -> {
            Map<String, List<Gauge>> removed = capacityGroupsMetrics.remove(absent);
            removed.values().forEach(gauges -> gauges.forEach(gauge -> gauge.set(0)));
        });
    }

    private void updateCapacityGroupCounters(Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms, Map<String, Tier> tierMap) {
        capacityGroupsHistograms.forEach((capacityGroup, histograms) -> {
            Id baseId = registry.createId(
                    TASK_METRIC_NAME,
                    "tier", tierMap.get(capacityGroup).name(),
                    "capacityGroup", capacityGroup
            );
            Map<String, List<Gauge>> capacityMetricsByState = capacityGroupsMetrics.computeIfAbsent(capacityGroup, k -> new HashMap<>());
            for (String state : TRACKED_STATES) {
                List<Gauge> updatedGauges = updateStateCounters(baseId, state, histograms.get(state), capacityMetricsByState.get(state));
                if (updatedGauges.isEmpty()) {
                    capacityMetricsByState.remove(capacityGroup);
                } else {
                    capacityMetricsByState.put(state, updatedGauges);
                }
            }
        });
    }

    private List<Gauge> updateStateCounters(Id baseId, String state, Histogram.Builder histogramBuilder, List<Gauge> gauges) {
        if (histogramBuilder == null) {
            // Nothing running for this state, reset gauges
            if (gauges != null) {
                gauges.forEach(g -> g.set(0));
            }
            return Collections.emptyList();
        }

        List<Long> counters = histogramBuilder.build().getCounters();

        // First time we have data for this capacity group.
        if (gauges == null) {
            Id id = baseId.withTag("state", state);
            List<Long> valueBounds = HISTOGRAM_DESCRIPTOR.getValueBounds();
            List<Gauge> newGauges = new ArrayList<>();
            for (int i = 0; i <= valueBounds.size(); i++) {
                Gauge newGauge;
                if (i < valueBounds.size()) {
                    long delayMs = valueBounds.get(i);
                    newGauge = registry.gauge(id.withTag("delay", DateTimeExt.toTimeUnitString(delayMs)));
                } else {
                    newGauge = registry.gauge(id.withTag("delay", "Unlimited"));
                }
                newGauge.set(counters.get(i));
                newGauges.add(newGauge);
            }
            return newGauges;
        }

        // Update gauges
        for (int i = 0; i < counters.size(); i++) {
            gauges.get(i).set(counters.get(i));
        }

        return gauges;
    }

    private Map<String, Map<String, Histogram.Builder>> buildCapacityGroupsHistograms(Set<String> capacityGroups) {
        Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms = newCapacityHistograms(capacityGroups);
        v2JobOperations.getAllJobMgrs()
                .forEach(jmgr -> resolveCapacityGroup(jmgr, capacityGroupsHistograms).ifPresent(capacityGroup ->
                        buildCapacityGroupHistogram(jmgr, capacityGroup, capacityGroupsHistograms)
                ));
        return capacityGroupsHistograms;
    }

    private void buildCapacityGroupHistogram(V2JobMgrIntf jmgr, String capacityGroup, Map<String, Map<String, Histogram.Builder>> capacityGroupsHistograms) {
        // 'capacityGroupsHistograms' is pre-initialized, but to avoid race condition we make extra check here.
        Map<String, Histogram.Builder> capacityGroupHistograms = capacityGroupsHistograms.computeIfAbsent(capacityGroup, k -> new HashMap<>());
        jmgr.getWorkers().forEach(worker -> {
            long timestamp = getTimestamp(worker);
            if (timestamp > 0) {
                long durationMs = System.currentTimeMillis() - timestamp;
                capacityGroupHistograms.computeIfAbsent(
                        worker.getState().name(),
                        name -> Histogram.newBuilder(HISTOGRAM_DESCRIPTOR)
                ).increment(durationMs);
            }
        });
    }

    private Optional<String> resolveCapacityGroup(V2JobMgrIntf jmgr, Map<String, Map<String, Histogram.Builder>> capacityHistograms) {
        V2JobMetadata jobMetadata = jmgr.getJobMetadata();
        if (jobMetadata == null) {
            return Optional.empty();
        }
        String capacityGroup = Parameters.getCapacityGroup(jobMetadata.getParameters());
        if (capacityGroup == null) {
            capacityGroup = Parameters.getAppName(jobMetadata.getParameters());
        }
        if (capacityGroup == null) {
            return Optional.of(ApplicationSlaManagementService.DEFAULT_APPLICATION);
        }
        return Optional.of(capacityHistograms.containsKey(capacityGroup) ? capacityGroup : ApplicationSlaManagementService.DEFAULT_APPLICATION);
    }

    private Map<String, Tier> buildTierMap() {
        return applicationSlaManagementService.getApplicationSLAs().stream()
                .collect(Collectors.toMap(ApplicationSLA::getAppName, ApplicationSLA::getTier));
    }

    private Map<String, Map<String, Histogram.Builder>> newCapacityHistograms(Set<String> capacityGroups) {
        return capacityGroups.stream().collect(Collectors.toMap(name -> name, name -> new HashMap<>()));
    }

    private long getTimestamp(V2WorkerMetadata worker) {
        V2JobState state = worker.getState();
        long timestamp;
        switch (state) {
            case Accepted:
                timestamp = worker.getAcceptedAt();
                break;
            case Launched:
                timestamp = worker.getLaunchedAt();
                break;
            case StartInitiated:
                timestamp = worker.getStartingAt();
                break;
            case Started:
                timestamp = worker.getStartedAt();
                break;
            case Failed:
            case Completed:
            case Noop:
            default:
                timestamp = -1;
        }
        return timestamp;
    }
}
