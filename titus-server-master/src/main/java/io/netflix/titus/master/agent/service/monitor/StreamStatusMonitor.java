package io.netflix.titus.master.agent.service.monitor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementException;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.common.util.rx.RetryHandlerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.PublishSubject;

/**
 * Helper {@link AgentStatusMonitor} implementation for handling streamed status updates.
 */
public class StreamStatusMonitor implements AgentStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(StreamStatusMonitor.class);

    private static final long RETRY_DELAYS_MS = 1_000;

    private final AgentStatusMonitorMetrics metrics;
    private final Subscription statusUpdateSubscription;

    private final PublishSubject<AgentStatus> statusUpdateSubject = PublishSubject.create();
    private volatile ConcurrentMap<String, AgentStatus> instanceStatuses = new ConcurrentHashMap<>();

    public StreamStatusMonitor(String source,
                               Observable<AgentStatus> agentStatusObservable,
                               Registry registry,
                               Scheduler scheduler) {
        this.metrics = new AgentStatusMonitorMetrics(source, registry);

        this.statusUpdateSubscription = agentStatusObservable
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(RETRY_DELAYS_MS, RETRY_DELAYS_MS, TimeUnit.MILLISECONDS)
                        .withScheduler(scheduler)
                        .buildExponentialBackoff()
                ).subscribe(this::handleStatusUpdate);
    }

    public void shutdown() {
        statusUpdateSubscription.unsubscribe();
    }

    @Override
    public AgentStatus getStatus(String agentInstanceId) {
        AgentStatus agentStatus = instanceStatuses.get(agentInstanceId);
        if (agentStatus == null) {
            throw AgentManagementException.agentNotFound(agentInstanceId);
        }
        return agentStatus;
    }

    @Override
    public Observable<AgentStatus> monitor() {
        return statusUpdateSubject.asObservable();
    }

    private void handleStatusUpdate(AgentStatus statusUpdate) {
        if (statusUpdate.getStatusCode() == AgentStatus.AgentStatusCode.Terminated) {
            instanceStatuses.remove(statusUpdate.getAgentInstance().getId());
        } else {
            instanceStatuses.put(statusUpdate.getAgentInstance().getId(), statusUpdate);
        }
        metrics.statusChanged(statusUpdate);
        logger.info("Status update: {} -> {} (source {})", statusUpdate.getAgentInstance(), statusUpdate.getStatusCode(), statusUpdate.getSourceId());
        statusUpdateSubject.onNext(statusUpdate);
    }
}
