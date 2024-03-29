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

package com.netflix.titus.runtime.eviction.endpoint.grpc;

import java.time.Duration;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.eviction.model.event.EvictionSnapshotEndEvent;
import com.netflix.titus.api.eviction.model.event.TaskTerminationEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.reference.TierReference;
import com.netflix.titus.grpc.protogen.EvictionQuota;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.Reference;

public final class GrpcEvictionModelConverters {

    private static final LoadingCache<com.netflix.titus.api.model.reference.Reference, Reference> CORE_TO_GRPC_REFERENCE_CACHE = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .build(coreReference -> {
                switch (coreReference.getLevel()) {
                    case System:
                        return Reference.newBuilder().setSystem(Reference.System.getDefaultInstance()).build();
                    case Tier:
                        return Reference.newBuilder().setTier(toGrpcTier(((TierReference) coreReference).getTier())).build();
                    case CapacityGroup:
                        return Reference.newBuilder().setCapacityGroup(coreReference.getName()).build();
                    case Job:
                    case Task:
                    default:
                }
                throw new IllegalArgumentException("not implemented yet");
            });

    private GrpcEvictionModelConverters() {
    }

    public static com.netflix.titus.api.model.reference.Reference toCoreReference(Reference grpcEntity) {
        switch (grpcEntity.getReferenceCase()) {
            case SYSTEM:
                return com.netflix.titus.api.model.reference.Reference.system();
            case TIER:
                return com.netflix.titus.api.model.reference.Reference.tier(toCoreTier(grpcEntity.getTier()));
            case CAPACITYGROUP:
                return com.netflix.titus.api.model.reference.Reference.capacityGroup(grpcEntity.getCapacityGroup());
            case JOBID:
                return com.netflix.titus.api.model.reference.Reference.job(grpcEntity.getJobId());
            case TASKID:
                return com.netflix.titus.api.model.reference.Reference.task(grpcEntity.getTaskId());
            case REFERENCE_NOT_SET:
        }
        throw new IllegalArgumentException("No mapping for: " + grpcEntity);
    }

    public static Reference toGrpcReference(com.netflix.titus.api.model.reference.Reference coreReference) {
        switch (coreReference.getLevel()) {
            case System:
            case Tier:
            case CapacityGroup:
                return CORE_TO_GRPC_REFERENCE_CACHE.get(coreReference);
            case Job:
                return Reference.newBuilder().setJobId(coreReference.getName()).build();
            case Task:
                return Reference.newBuilder().setTaskId(coreReference.getName()).build();
        }
        throw new IllegalArgumentException("No GRPC mapping for: " + coreReference);
    }

    public static com.netflix.titus.api.eviction.model.EvictionQuota toCoreEvictionQuota(EvictionQuota grpcEntity) {
        return com.netflix.titus.api.eviction.model.EvictionQuota.newBuilder()
                .withReference(toCoreReference(grpcEntity.getTarget()))
                .withQuota(grpcEntity.getQuota())
                .withMessage(grpcEntity.getMessage())
                .build();
    }

    public static EvictionQuota toGrpcEvictionQuota(com.netflix.titus.api.eviction.model.EvictionQuota evictionQuota) {
        return EvictionQuota.newBuilder()
                .setTarget(toGrpcReference(evictionQuota.getReference()))
                .setQuota((int) evictionQuota.getQuota())
                .setMessage(evictionQuota.getMessage())
                .build();
    }

    public static EvictionEvent toCoreEvent(EvictionServiceEvent grpcEvent) {
        switch (grpcEvent.getEventCase()) {
            case SNAPSHOTEND:
                return EvictionEvent.newSnapshotEndEvent();
            case EVICTIONQUOTAEVENT:
                return EvictionEvent.newQuotaEvent(toCoreEvictionQuota(grpcEvent.getEvictionQuotaEvent().getQuota()));
            case TASKTERMINATIONEVENT:
                EvictionServiceEvent.TaskTerminationEvent taskTermination = grpcEvent.getTaskTerminationEvent();
                if (taskTermination.getApproved()) {
                    return EvictionEvent.newSuccessfulTaskTerminationEvent(taskTermination.getTaskId(), taskTermination.getReason());
                }
                return EvictionEvent.newFailedTaskTerminationEvent(
                        taskTermination.getTaskId(),
                        taskTermination.getReason(),
                        EvictionException.deconstruct(taskTermination.getRestrictionCode(), taskTermination.getRestrictionMessage())
                );
            case EVENT_NOT_SET:
        }
        throw new IllegalArgumentException("No mapping for: " + grpcEvent);
    }

    public static Optional<EvictionServiceEvent> toGrpcEvent(EvictionEvent coreEvent) {
        if (coreEvent instanceof EvictionSnapshotEndEvent) {
            EvictionServiceEvent grpcEvent = EvictionServiceEvent.newBuilder()
                    .setSnapshotEnd(EvictionServiceEvent.SnapshotEnd.getDefaultInstance())
                    .build();
            return Optional.of(grpcEvent);
        }
        if (coreEvent instanceof EvictionQuotaEvent) {
            EvictionQuotaEvent actualEvent = (EvictionQuotaEvent) coreEvent;
            EvictionServiceEvent grpcEvent = EvictionServiceEvent.newBuilder()
                    .setEvictionQuotaEvent(EvictionServiceEvent.EvictionQuotaEvent.newBuilder()
                            .setQuota(toGrpcEvictionQuota(actualEvent.getQuota()))
                            .build()
                    )
                    .build();
            return Optional.of(grpcEvent);
        }
        if (coreEvent instanceof TaskTerminationEvent) {
            TaskTerminationEvent actualEvent = (TaskTerminationEvent) coreEvent;

            EvictionServiceEvent.TaskTerminationEvent.Builder eventBuilder = EvictionServiceEvent.TaskTerminationEvent.newBuilder()
                    .setTaskId(actualEvent.getTaskId())
                    .setApproved(actualEvent.isApproved());

            if (!actualEvent.isApproved()) {
                Throwable error = actualEvent.getError().get();
                if (error instanceof EvictionException) {
                    EvictionException evictionException = (EvictionException) error;
                    eventBuilder.setRestrictionCode("" + evictionException.getErrorCode());
                } else {
                    eventBuilder.setRestrictionCode("" + EvictionException.ErrorCode.Unknown);
                }
                eventBuilder.setRestrictionMessage(error.getMessage());
            }

            EvictionServiceEvent grpcEvent = EvictionServiceEvent.newBuilder().setTaskTerminationEvent(eventBuilder.build()).build();
            return Optional.of(grpcEvent);
        }
        return Optional.empty();
    }

    public static Tier toCoreTier(com.netflix.titus.grpc.protogen.Tier grpcTier) {
        switch (grpcTier) {
            case Flex:
                return Tier.Flex;
            case Critical:
                return Tier.Critical;
        }
        return Tier.Flex; // Default to flex
    }

    public static com.netflix.titus.grpc.protogen.Tier toGrpcTier(Tier tier) {
        switch (tier) {
            case Critical:
                return com.netflix.titus.grpc.protogen.Tier.Critical;
            case Flex:
                return com.netflix.titus.grpc.protogen.Tier.Flex;
        }
        throw new IllegalArgumentException("Unrecognized Tier value: " + tier);
    }
}
