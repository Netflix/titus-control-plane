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

package com.netflix.titus.master.endpoint.v2.rest;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.api.endpoint.v2.rest.representation.ReservationUsage;
import com.netflix.titus.api.endpoint.v2.rest.representation.TierRepresentation;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;

/**
 * Translate between REST representation and core entities.
 */
public final class Representation2ModelConvertions {

    private Representation2ModelConvertions() {
    }

    public static ApplicationSLA asCoreEntity(ApplicationSlaRepresentation representation) {
        ResourceDimension resourceDimension = ResourceDimension.newBuilder()
                .withCpus(representation.getInstanceCPU())
                .withMemoryMB(representation.getInstanceMemoryMB())
                .withDiskMB(representation.getInstanceDiskMB())
                .withNetworkMbs(representation.getInstanceNetworkMbs())
                .build();

        Tier tier;
        if (representation.getTier() != null) {
            tier = Tier.valueOf(representation.getTier().name());
        } else {
            tier = Tier.Flex;
        }

        return ApplicationSLA.newBuilder()
                .withAppName(representation.getAppName())
                .withTier(tier)
                .withResourceDimension(resourceDimension)
                .withInstanceCount(representation.getInstanceCount())
                .build();
    }

    public static ApplicationSlaRepresentation asRepresentation(ApplicationSLA coreEntity) {
        return asRepresentation(coreEntity, null, null);
    }

    public static ApplicationSlaRepresentation asRepresentation(ApplicationSLA coreEntity,
                                                                String cellId,
                                                                ReservationUsage reservationUsage) {
        ResourceDimension resourceDimension = coreEntity.getResourceDimension();
        return new ApplicationSlaRepresentation(
                coreEntity.getAppName(),
                cellId,
                TierRepresentation.valueOf(coreEntity.getTier().name()),
                resourceDimension.getCpu(),
                resourceDimension.getMemoryMB(),
                resourceDimension.getDiskMB(),
                resourceDimension.getNetworkMbs(),
                coreEntity.getInstanceCount(),
                reservationUsage
        );
    }
}
