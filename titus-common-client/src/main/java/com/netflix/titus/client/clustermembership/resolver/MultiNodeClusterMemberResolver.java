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

package com.netflix.titus.client.clustermembership.resolver;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipSnapshot;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.rx.ReactorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

/**
 * {@link MultiNodeClusterMemberResolver} reads cluster membership data directly from the cluster members, by invoking
 * the cluster membership API. This implementation is agnostic to a specific consistent store used to orchestrate the
 * data exchange and the leader election process. If this approach is suboptimal, {@link ClusterMemberResolver}
 * should be integrated directly with the underlying store.
 *
 * <h1>Rules for adding a member</h1>
 * A new member is added if any of the conditions below is met:
 * <ul>
 *     <ul>It is a seed node</ul>
 *     <ul>It is reported by a healthy member(s)</ul>
 * </ul>
 * <h1>Rules for removing a member</h1>
 * The following rules are applied to identify a known cluster member M that should be removed. All the conditions
 * must hold true for the node to be removed:
 * <ul>
 *     <li>It is not a seed node</li>
 *     <li>None of the healthy cluster members reports M as a member</li>
 *     <li>It is not possible to connect to M for a prolonged amount of time</li>
 * </ul>
 * <p>
 * TODO Reactive data update from the underlying sources. Now it is updated periodically on the configured schedule only.
 */
public class MultiNodeClusterMemberResolver implements ClusterMemberResolver {

    private static final Logger logger = LoggerFactory.getLogger(MultiNodeClusterMemberResolver.class);

    private static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(MultiNodeClusterMemberResolver.class.getSimpleName())
            .withDescription("Cluster membership resolver aggregating data from all cluster members")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofSeconds(1))
            .withTimeout(Duration.ofSeconds(5))
            .withRetryerSupplier(Retryers::never)
            .withOnErrorHandler((action, error) -> {
                logger.warn("Cannot update cluster membership data: {}", error.getMessage());
                logger.debug(error.getMessage(), error);
            })
            .build();

    private final Supplier<Set<ClusterMemberAddress>> seedAddressesProvider;
    private final Function<ClusterMemberAddress, DirectClusterMemberResolver> directResolverFactory;
    private final Function<ClusterMember, ClusterMemberAddress> addressSelector;

    private final ConcurrentMap<String, DirectClusterMemberResolver> memberResolversByIpAddress = new ConcurrentHashMap<>();
    private final ScheduleReference scheduleReference;
    private volatile ClusterMembershipSnapshot lastReportedSnapshot;

    private final ReplayProcessor<ClusterMembershipSnapshot> snapshotEventProcessor = ReplayProcessor.create(1);
    private final Flux<ClusterMembershipSnapshot> snapshotEventObservable = snapshotEventProcessor
            .distinctUntilChanged()
            .onBackpressureError()
            .compose(ReactorExt.badSubscriberHandler(logger));

    public MultiNodeClusterMemberResolver(ClusterMembershipResolverConfiguration configuration,
                                          Supplier<Set<ClusterMemberAddress>> seedAddressesProvider,
                                          Function<ClusterMemberAddress, DirectClusterMemberResolver> directResolverFactory,
                                          Function<ClusterMember, ClusterMemberAddress> addressSelector,
                                          TitusRuntime titusRuntime) {
        this.seedAddressesProvider = seedAddressesProvider;
        this.directResolverFactory = directResolverFactory;
        this.addressSelector = addressSelector;

        this.scheduleReference = titusRuntime.getLocalScheduler().schedule(
                SCHEDULE_DESCRIPTOR.toBuilder()
                        .withInterval(Duration.ofMillis(configuration.getMultiMemberRefreshIntervalMs()))
                        .build(),
                context -> {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    refresh();

                    ClusterMembershipSnapshot newSnapshot = getSnapshot();
                    report(newSnapshot);
                    snapshotEventProcessor.onNext(newSnapshot);

                    logger.debug("Refreshed data in {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
                },
                false
        );
    }

    public void shutdown() {
        scheduleReference.cancel();
    }

    /**
     * Returns cluster membership snapshot computed from data provided by all connected and healthy members.
     */
    @Override
    public ClusterMembershipSnapshot getSnapshot() {
        if (memberResolversByIpAddress.isEmpty()) {
            return ClusterMembershipSnapshot.empty();
        }
        // Each cluster member should be reported by by each resolver.
        return buildSnapshot(findHealthySnapshots());
    }

    @Override
    public Flux<ClusterMembershipSnapshot> resolve() {
        return snapshotEventObservable;
    }

    private List<DirectClusterMemberResolver> findHealthyMembers() {
        return memberResolversByIpAddress.values().stream().filter(DirectClusterMemberResolver::isHealthy).collect(Collectors.toList());
    }

    private List<ClusterMembershipSnapshot> findHealthySnapshots() {
        return findHealthyMembers().stream().map(DirectClusterMemberResolver::getSnapshot).collect(Collectors.toList());
    }

    private ClusterMembershipSnapshot buildSnapshot(List<ClusterMembershipSnapshot> healthySnapshots) {
        ClusterMembershipSnapshot.Builder builder = ClusterMembershipSnapshot.newBuilder();

        Map<String, List<ClusterMembershipRevision<ClusterMember>>> grouped = healthySnapshots.stream()
                .flatMap(snapshot -> snapshot.getMemberRevisions().values().stream())
                .collect(Collectors.groupingBy(m -> m.getCurrent().getMemberId()));

        List<ClusterMembershipRevision<ClusterMember>> recentRevisions = grouped.values().stream()
                .map(this::findBestMemberRevision)
                .collect(Collectors.toList());
        builder.withMemberRevisions(recentRevisions);

        // Find leader
        Optional<ClusterMembershipRevision<ClusterMemberLeadership>> recentLeader = Optional.empty();
        for (ClusterMembershipSnapshot snapshot : healthySnapshots) {
            if (snapshot.getLeaderRevision().isPresent()) {
                if (recentLeader.isPresent()) {
                    if (recentLeader.get().getRevision() < snapshot.getLeaderRevision().get().getRevision()) {
                        recentLeader = snapshot.getLeaderRevision();
                    }
                } else {
                    recentLeader = snapshot.getLeaderRevision();
                }
            }
        }
        recentLeader.ifPresent(builder::withLeaderRevision);

        // Choose latest version of each
        long minStaleness = healthySnapshots.stream()
                .mapToLong(ClusterMembershipSnapshot::getStalenessMs)
                .min()
                .orElse(0);
        builder.withStalenessMs(minStaleness);

        return builder.build();
    }

    private ClusterMembershipRevision<ClusterMember> findBestMemberRevision(List<ClusterMembershipRevision<ClusterMember>> singleMemberVersions) {
        Preconditions.checkArgument(singleMemberVersions.size() > 0);

        long bestRevision = singleMemberVersions.get(0).getRevision();
        ClusterMembershipRevision<ClusterMember> best = singleMemberVersions.get(0);

        for (int i = 1; i < singleMemberVersions.size(); i++) {
            ClusterMembershipRevision<ClusterMember> next = singleMemberVersions.get(i);
            if (next.getRevision() > bestRevision) {
                bestRevision = next.getRevision();
                best = next;
            }
        }
        return best;
    }

    private void refresh() {
        // Always first add missing seed nodes.
        addNewSeeds();

        // If no healthy member, we cannot make any progress, so exit.
        List<DirectClusterMemberResolver> healthyMemberResolvers = findHealthyMembers();
        if (healthyMemberResolvers.isEmpty()) {
            logger.debug("Cannot connect to any cluster member. Known members: {}", toResolvedMembersString());
            return;
        }
        ClusterMembershipSnapshot resolvedSnapshot = buildSnapshot(healthyMemberResolvers.stream()
                .map(DirectClusterMemberResolver::getSnapshot)
                .collect(Collectors.toList()));

        Map<String, ClusterMembershipRevision<ClusterMember>> resolvedMembersByIp = resolvedSnapshot.getMemberRevisions().values().stream()
                .collect(Collectors.toMap(r -> addressSelector.apply(r.getCurrent()).getIpAddress(), r -> r));

        // Find terminated members.
        Set<String> toRemove = memberResolversByIpAddress.keySet().stream()
                .filter(ip -> !resolvedMembersByIp.containsKey(ip) && !isSeedIp(ip))
                .collect(Collectors.toSet());
        if (!toRemove.isEmpty()) {
            logger.info("Removing terminated cluster members: {}", toRemove);
            toRemove.forEach(ip -> {
                DirectClusterMemberResolver removed = memberResolversByIpAddress.remove(ip);
                if (removed != null) {
                    removed.shutdown();
                }
            });
        }

        // Find new members that we should connect to.
        Set<String> toAdd = resolvedMembersByIp.keySet().stream()
                .filter(ip -> !memberResolversByIpAddress.containsKey(ip))
                .collect(Collectors.toSet());
        if (!toAdd.isEmpty()) {
            logger.info("Adding new cluster members: {}", toAdd);
            toAdd.forEach(ip -> memberResolversByIpAddress.put(
                    ip,
                    directResolverFactory.apply(addressSelector.apply(resolvedMembersByIp.get(ip).getCurrent())))
            );
        }
    }

    private void addNewSeeds() {
        Set<ClusterMemberAddress> seeds = seedAddressesProvider.get();
        Map<String, ClusterMemberAddress> seedsByIps = seeds.stream().collect(Collectors.toMap(ClusterMemberAddress::getIpAddress, m -> m));
        Set<String> toAdd = CollectionsExt.copyAndRemove(seedsByIps.keySet(), memberResolversByIpAddress.keySet());
        if (!toAdd.isEmpty()) {
            logger.info("Discovered new seed nodes with IPs: {}", toAdd);
            toAdd.forEach(ip -> memberResolversByIpAddress.put(ip, directResolverFactory.apply(seedsByIps.get(ip))));
        }
    }

    private boolean isSeedIp(String ipAddress) {
        Optional<ClusterMemberAddress> seedAddress = seedAddressesProvider.get().stream().filter(a -> a.getIpAddress().equals(ipAddress)).findFirst();
        return seedAddress.isPresent();
    }

    private String toResolvedMembersString() {
        return String.format("[%s]",
                memberResolversByIpAddress.values().stream()
                        .map(m -> m.getPrintableName() + '@' + m.getAddress().getIpAddress())
                        .collect(Collectors.joining(", "))
        );
    }

    private void report(ClusterMembershipSnapshot newSnapshot) {
        if (lastReportedSnapshot == null || !lastReportedSnapshot.equals(newSnapshot)) {
            logger.info("Cluster membership change: {}", newSnapshot);
        }

        this.lastReportedSnapshot = newSnapshot;
    }
}
