package io.netflix.titus.master.agent.service.monitor;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamStatusMonitorTest {

    private static final String MY_SOURCE = "TEST";

    private final TestScheduler testScheduler = Schedulers.test();

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();

    private final StreamStatusMonitor monitor = new StreamStatusMonitor(MY_SOURCE, statusUpdateSubject, new DefaultRegistry(), testScheduler);

    private final ExtTestSubscriber<AgentStatus> testSubscriber = new ExtTestSubscriber<>();

    private final AgentInstance instance = AgentGenerator.agentInstances().getValue();

    @Before
    public void setUp() throws Exception {
        monitor.monitor().subscribe(testSubscriber);
    }

    @Test
    public void testStatusUpdatePropagation() throws Exception {
        statusUpdateSubject.onNext(AgentStatus.healthy(MY_SOURCE, instance, "OK", testScheduler.now()));
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
        assertThat(monitor.getStatus(instance.getId()).getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
    }

    @Test(expected = AgentManagementException.class)
    public void testRemovedInstanceCleanup() throws Exception {
        statusUpdateSubject.onNext(AgentStatus.healthy(MY_SOURCE, instance, "OK", testScheduler.now()));
        statusUpdateSubject.onNext(AgentStatus.terminated(MY_SOURCE, instance, "Terminated", testScheduler.now()));

        monitor.getStatus(instance.getId());
    }
}