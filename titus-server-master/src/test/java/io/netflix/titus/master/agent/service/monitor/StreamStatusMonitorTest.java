package io.netflix.titus.master.agent.service.monitor;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class StreamStatusMonitorTest {

    private static final String MY_SOURCE = "TEST";

    private final TestScheduler testScheduler = Schedulers.test();

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final StreamStatusMonitor monitor = new StreamStatusMonitor(MY_SOURCE, false, agentManagementService, statusUpdateSubject, new DefaultRegistry(), testScheduler);

    private final ExtTestSubscriber<AgentStatus> testSubscriber = new ExtTestSubscriber<>();

    private final AgentInstance instance = AgentGenerator.agentInstances().getValue();

    @Before
    public void setUp() throws Exception {
        monitor.monitor().subscribe(testSubscriber);
    }

    @Test
    public void testStatusUpdatePropagation() {
        statusUpdateSubject.onNext(AgentStatus.healthy(MY_SOURCE, instance, "OK", testScheduler.now()));
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
        assertThat(monitor.getStatus(instance.getId()).getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
    }

    @Test
    public void testRemovedInstanceCleanup() {
        statusUpdateSubject.onNext(AgentStatus.healthy(MY_SOURCE, instance, "OK", testScheduler.now()));
        statusUpdateSubject.onNext(AgentStatus.terminated(MY_SOURCE, instance, "Terminated", testScheduler.now()));

        AgentStatus status = monitor.getStatus(instance.getId());
        assertThat(status.getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
        assertThat(status.getDescription()).contains("No data recorded yet");
    }
}