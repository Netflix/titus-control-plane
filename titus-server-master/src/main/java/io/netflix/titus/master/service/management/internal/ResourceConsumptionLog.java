/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.service.management.internal;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.master.model.ResourceDimensions;
import io.netflix.titus.master.service.management.CompositeResourceConsumption;
import io.netflix.titus.master.service.management.ResourceConsumptionEvents;
import io.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupAllocationEvent;
import io.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupRemovedEvent;
import io.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupUndefinedEvent;
import io.netflix.titus.master.service.management.ResourceConsumptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

/**
 * A resource consumption state change event log.
 */
@Singleton
public class ResourceConsumptionLog {

    private static final Logger logger = LoggerFactory.getLogger(ResourceConsumptionLog.class);

    private final ResourceConsumptionService resourceConsumptionService;
    private Subscription subscription;

    @Inject
    public ResourceConsumptionLog(ResourceConsumptionService resourceConsumptionService) {
        this.resourceConsumptionService = resourceConsumptionService;
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Activating resource consumption logging...");
        this.subscription = resourceConsumptionService.resourceConsumptionEvents().subscribe(
                ResourceConsumptionLog::doLog,
                e -> logger.error("Resource consumption log terminated with an error", e),
                () -> logger.warn("Resource consumption log completed")
        );
        return Observable.empty();
    }

    @PreDestroy
    public void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
    }

    /* Visible for testing */
    static String doLog(ResourceConsumptionEvents.ResourceConsumptionEvent event) {
        if (event instanceof CapacityGroupAllocationEvent) {
            CapacityGroupAllocationEvent changeEvent = (CapacityGroupAllocationEvent) event;
            CompositeResourceConsumption consumption = changeEvent.getCapacityGroupConsumption();

            StringBuilder sb = new StringBuilder("Resource consumption change: group=");
            sb.append(consumption.getConsumerName());

            if (consumption.isAboveLimit()) {
                sb.append(" [above limit] ");
            } else {
                sb.append(" [below limit] ");
            }

            sb.append("actual=");
            ResourceDimensions.format(consumption.getCurrentConsumption(), sb);
            sb.append(", max=");
            ResourceDimensions.format(consumption.getMaxConsumption(), sb);
            sb.append(", limit=");
            ResourceDimensions.format(consumption.getAllowedConsumption(), sb);

            if (consumption.getAttributes().isEmpty()) {
                sb.append(", attrs={}");
            } else {
                sb.append(", attrs={");
                consumption.getAttributes().forEach((k, v) -> sb.append(k).append('=').append(v).append(','));
                sb.setCharAt(sb.length() - 1, '}');
            }

            String message = sb.toString();
            if (consumption.isAboveLimit()) {
                logger.warn(message);
            } else {
                logger.info(message);
            }
            return message;
        }

        if (event instanceof CapacityGroupUndefinedEvent) {
            String message = "Capacity group not defined: group=" + event.getCapacityGroup();
            logger.warn(message);
            return message;
        }

        if (event instanceof CapacityGroupRemovedEvent) {
            String message = "Capacity group no longer defined: group=" + event.getCapacityGroup();
            logger.info(message);
            return message;
        }

        String message = "Unrecognized resource consumption event type " + event.getClass();
        logger.error(message);
        return message;
    }
}
