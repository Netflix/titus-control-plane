package com.netflix.titus.runtime.eviction.endpoint.grpc;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.eviction.model.event.EvictionSnapshotEndEvent;
import com.netflix.titus.api.eviction.model.event.SystemDisruptionBudgetUpdateEvent;
import com.netflix.titus.api.eviction.model.event.TaskTerminationEvent;
import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.TokenBucketRefillPolicy;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.Reference;
import com.netflix.titus.grpc.protogen.SystemDisruptionBudget;
import com.netflix.titus.grpc.protogen.TokenBucketPolicy;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcAgentModelConverters.toCoreTier;

public final class GrpcEvictionModelConverters {

    private static final LoadingCache<com.netflix.titus.api.model.Reference, Reference> CORE_TO_GRPC_REFERENCE_CACHE = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .build(GrpcEvictionModelConverters::toGrpcReference);

    private GrpcEvictionModelConverters() {
    }

    public static com.netflix.titus.api.eviction.model.SystemDisruptionBudget toCoreSystemDisruptionBudget(SystemDisruptionBudget grpcEntity) {
        return com.netflix.titus.api.eviction.model.SystemDisruptionBudget.newBuilder()
                .withReference(toCoreReference(grpcEntity.getTarget()))
                .withTokenBucketDescriptor(toCoreTokenBucketDescriptor(grpcEntity.getAdmissionControlPolicy()))
                .build();
    }

    public static SystemDisruptionBudget toGrpcSystemDisruptionBudget(com.netflix.titus.api.eviction.model.SystemDisruptionBudget coreEntity) {
        return SystemDisruptionBudget.newBuilder()
                .setTarget(toGrpcReference(coreEntity.getReference()))
                .setAdmissionControlPolicy(toGrpcTokenBucketPolicy(coreEntity.getTokenBucketPolicy()))
                .build();
    }

    public static com.netflix.titus.api.model.Reference toCoreReference(Reference grpcEntity) {
        switch (grpcEntity.getReferenceCase()) {
            case GLOBAL:
                return com.netflix.titus.api.model.Reference.global();
            case TIER:
                return com.netflix.titus.api.model.Reference.tier(toCoreTier(grpcEntity.getTier()));
            case CAPACITYGROUP:
                return com.netflix.titus.api.model.Reference.capacityGroup(grpcEntity.getCapacityGroup());
            case JOBID:
            case TASKID:
            case REFERENCE_NOT_SET:
        }
        throw new IllegalArgumentException("No mapping for: " + grpcEntity);
    }

    public static Reference toGrpcReference(com.netflix.titus.api.model.Reference coreReference) {
        switch (coreReference.getLevel()) {
            case Global:
            case Tier:
            case CapacityGroup:
                return CORE_TO_GRPC_REFERENCE_CACHE.get(coreReference);
            case Job:
            case Task:
        }
        throw new IllegalArgumentException("No GRPC mapping for: " + coreReference);
    }

    public static com.netflix.titus.api.model.TokenBucketPolicy toCoreTokenBucketDescriptor(TokenBucketPolicy grpcEntity) {
        TokenBucketPolicy.FixedIntervalRefillStrategy grpcRefillPolicy = grpcEntity.getFixedIntervalRefillStrategy();
        TokenBucketRefillPolicy coreRefillPolicy = FixedIntervalTokenBucketRefillPolicy.newBuilder()
                .withIntervalMs(grpcRefillPolicy.getIntervalMs())
                .withNumberOfTokensPerInterval(grpcRefillPolicy.getRefillRate())
                .build();
        return com.netflix.titus.api.model.TokenBucketPolicy.newBuilder()
                .withCapacity(grpcEntity.getCapacity())
                .withInitialNumberOfTokens(grpcEntity.getInitialNumberOfTokens())
                .withRefillPolicy(coreRefillPolicy)
                .build();
    }

    public static TokenBucketPolicy toGrpcTokenBucketPolicy(com.netflix.titus.api.model.TokenBucketPolicy coreTokenBucketPolicy) {
        FixedIntervalTokenBucketRefillPolicy refillPolicy = (FixedIntervalTokenBucketRefillPolicy) coreTokenBucketPolicy.getRefillPolicy();
        return TokenBucketPolicy.newBuilder()
                .setCapacity(coreTokenBucketPolicy.getCapacity())
                .setInitialNumberOfTokens(coreTokenBucketPolicy.getInitialNumberOfTokens())
                .setFixedIntervalRefillStrategy(TokenBucketPolicy.FixedIntervalRefillStrategy.newBuilder()
                        .setIntervalMs(refillPolicy.getIntervalMs())
                        .setRefillRate(refillPolicy.getNumberOfTokensPerInterval())
                        .build()
                )
                .build();
    }

    public static com.netflix.titus.api.eviction.model.EvictionQuota toCoreEvictionQuota(EvictionQuota grpcEntity) {
        return com.netflix.titus.api.eviction.model.EvictionQuota.newBuilder()
                .withQuota(grpcEntity.getQuota())
                .withReference(toCoreReference(grpcEntity.getTarget()))
                .build();
    }

    public static EvictionQuota toGrpcEvictionQuota(com.netflix.titus.api.eviction.model.EvictionQuota evictionQuota) {
        return EvictionQuota.newBuilder()
                .setTarget(toGrpcReference(evictionQuota.getReference()))
                .setQuota((int) evictionQuota.getQuota())
                .build();
    }

    public static EvictionEvent toCoreEvent(EvictionServiceEvent grpcEvent) {
        switch (grpcEvent.getEventCase()) {
            case SNAPSHOTEND:
                return EvictionEvent.newSnapshotEndEvent();
            case SYSTEMDISRUPTIONBUDGETUPDATEEVENT:
                return EvictionEvent.newSystemDisruptionBudgetEvent(toCoreSystemDisruptionBudget(grpcEvent.getSystemDisruptionBudgetUpdateEvent().getCurrent()));
            case EVICTIONQUOTAEVENT:
                return EvictionEvent.newQuotaEvent(toCoreEvictionQuota(grpcEvent.getEvictionQuotaEvent().getQuota()));
            case TASKTERMINATIONEVENT:
                return EvictionEvent.newTaskTerminationEvent(grpcEvent.getTaskTerminationEvent().getTaskId(), grpcEvent.getTaskTerminationEvent().getApproved());
            case EVENT_NOT_SET:
        }
        throw new IllegalArgumentException("No mapping for: " + grpcEvent);
    }

    public static EvictionServiceEvent toGrpcEvent(EvictionEvent coreEvent) {
        if (coreEvent instanceof EvictionSnapshotEndEvent) {
            return EvictionServiceEvent.newBuilder()
                    .setSnapshotEnd(EvictionServiceEvent.SnapshotEnd.getDefaultInstance())
                    .build();
        }
        if (coreEvent instanceof SystemDisruptionBudgetUpdateEvent) {
            SystemDisruptionBudgetUpdateEvent actualEvent = (SystemDisruptionBudgetUpdateEvent) coreEvent;
            return EvictionServiceEvent.newBuilder()
                    .setSystemDisruptionBudgetUpdateEvent(EvictionServiceEvent.SystemDisruptionBudgetUpdateEvent.newBuilder()
                            .setCurrent(toGrpcSystemDisruptionBudget(actualEvent.getSystemDisruptionBudget()))
                            .build()
                    )
                    .build();
        }
        if (coreEvent instanceof EvictionQuotaEvent) {
            EvictionQuotaEvent actualEvent = (EvictionQuotaEvent) coreEvent;
            return EvictionServiceEvent.newBuilder()
                    .setEvictionQuotaEvent(EvictionServiceEvent.EvictionQuotaEvent.newBuilder()
                            .setQuota(toGrpcEvictionQuota(actualEvent.getQuota()))
                            .build()
                    )
                    .build();
        }
        if (coreEvent instanceof TaskTerminationEvent) {
            TaskTerminationEvent actualEvent = (TaskTerminationEvent) coreEvent;
            return EvictionServiceEvent.newBuilder()
                    .setTaskTerminationEvent(EvictionServiceEvent.TaskTerminationEvent.newBuilder()
                            .setTaskId(actualEvent.getTaskId())
                            .setApproved(actualEvent.isApproved())
                            .build()
                    )
                    .build();
        }
        throw new IllegalArgumentException("No GRPC mapping for: " + coreEvent);
    }
}
