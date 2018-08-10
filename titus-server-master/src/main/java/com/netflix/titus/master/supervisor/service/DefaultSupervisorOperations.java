package com.netflix.titus.master.supervisor.service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.event.MasterInstanceUpdateEvent;
import com.netflix.titus.master.supervisor.model.event.SupervisorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

@Singleton
public class DefaultSupervisorOperations implements SupervisorOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSupervisorOperations.class);

    private final MasterMonitor masterMonitor;
    private final Subject<SupervisorEvent, SupervisorEvent> eventSubject = new SerializedSubject<>(PublishSubject.create());
    private final Observable<SupervisorEvent> eventObservable = ObservableExt.protectFromMissingExceptionHandlers(eventSubject, logger);

    @Inject
    public DefaultSupervisorOperations(MasterMonitor masterMonitor) {
        this.masterMonitor = masterMonitor;
    }

    @Override
    public List<MasterInstance> getMasterInstances() {
        return masterMonitor.observeMasters().take(1).toBlocking().first();
    }

    @Override
    public Optional<MasterInstance> findMasterInstance(String instanceId) {
        return getMasterInstances().stream()
                .filter(i -> i.getInstanceId().equals(instanceId))
                .findFirst();
    }

    @Override
    public MasterInstance getMasterInstance(String instanceId) {
        return findMasterInstance(instanceId).orElseThrow(() -> SupervisorServiceException.masterInstanceNotFound(instanceId));
    }

    @Override
    public Optional<MasterInstance> findLeader() {
        return getMasterInstances().stream().filter(i -> MasterState.isLeader(i.getStatus().getState())).findFirst();
    }

    @Override
    public Observable<SupervisorEvent> events(boolean includeSnapshot) {
        if (includeSnapshot) {
            return eventObservable.compose(ObservableExt.head(() ->
                    getMasterInstances().stream().map(MasterInstanceUpdateEvent::new).collect(Collectors.toList())
            ));
        } else {
            return eventObservable;
        }
    }
}
