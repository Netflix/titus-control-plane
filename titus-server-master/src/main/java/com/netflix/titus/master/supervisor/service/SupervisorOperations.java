package com.netflix.titus.master.supervisor.service;

import java.util.List;
import java.util.Optional;

import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.event.SupervisorEvent;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import rx.Observable;

public interface SupervisorOperations {

    String COMPONENT = "supervisor";

    List<MasterInstance> getMasterInstances();

    Optional<MasterInstance> findMasterInstance(String instanceId);

    MasterInstance getMasterInstance(String instanceId);

    Optional<MasterInstance> findLeader();

    Observable<SupervisorEvent> events();

    void stopBeingLeader(CallMetadata callMetadata);
}
