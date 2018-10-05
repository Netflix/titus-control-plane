package com.netflix.titus.testkit.junit.asserts;

import java.util.List;
import java.util.function.Predicate;

import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthSnapshotEvent;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthUpdateEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public final class ContainerHealthAsserts {

    @SafeVarargs
    public static void assertContainerHealthSnapshot(ContainerHealthEvent event, Predicate<ContainerHealthStatus>... predicates) {
        assertThat(event).isInstanceOf(ContainerHealthSnapshotEvent.class);
        ContainerHealthSnapshotEvent snapshotEvent = (ContainerHealthSnapshotEvent) event;

        List<ContainerHealthStatus> snapshotEvents = snapshotEvent.getSnapshot();
        assertThat(snapshotEvents).describedAs("Expecting %s events, but got %s", predicates.length, snapshotEvents.size()).hasSize(predicates.length);

        for (int i = 0; i < snapshotEvents.size(); i++) {
            if (!predicates[i].test(snapshotEvents.get(i))) {
                fail("Event %s does not match its predicate: event=%s", i, snapshotEvents.get(i));
            }
        }
    }

    public static void assertContainerHealth(ContainerHealthStatus healthStatus, String expectedTaskId, ContainerHealthState expectedHealthState) {
        assertThat(healthStatus.getTaskId()).isEqualTo(expectedTaskId);
        assertThat(healthStatus.getState()).isEqualTo(expectedHealthState);
    }

    public static void assertContainerHealthEvent(ContainerHealthEvent event, String expectedTaskId, ContainerHealthState expectedHealthState) {
        assertThat(event).isInstanceOf(ContainerHealthUpdateEvent.class);
        assertContainerHealth(((ContainerHealthUpdateEvent) event).getContainerHealthStatus(), expectedTaskId, expectedHealthState);
    }
}
