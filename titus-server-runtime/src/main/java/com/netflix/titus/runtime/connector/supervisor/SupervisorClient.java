package com.netflix.titus.runtime.connector.supervisor;

import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.grpc.protogen.MasterInstances;
import com.netflix.titus.grpc.protogen.SupervisorEvent;
import rx.Observable;

public interface SupervisorClient {

    Observable<MasterInstances> getMasterInstances();

    Observable<MasterInstance> getMasterInstance(String instanceId);

    Observable<SupervisorEvent> observeEvents();
}
