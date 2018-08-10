package com.netflix.titus.master.supervisor.service.leader;

import java.util.Collections;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import rx.Observable;

@Singleton
public class ImmediateLocalMasterInstanceResolver implements LocalMasterInstanceResolver {

    private final MasterInstance ownMasterInstance;

    @Inject
    public ImmediateLocalMasterInstanceResolver(MasterDescription masterDescription) {
        this.ownMasterInstance = MasterInstance.newBuilder()
                .withInstanceId(masterDescription.getHostIP())
                .withIpAddress(masterDescription.getHostIP())
                .withStatus(MasterStatus.newBuilder()
                        .withState(MasterState.LeaderActivated)
                        .withReasonCode("none")
                        .withReasonMessage("Embedded TitusMaster activated")
                        .build()
                )
                .withStatusHistory(Collections.emptyList())
                .build();
    }

    @Override
    public Observable<MasterInstance> observeLocalMasterInstanceUpdates() {
        return Observable.just(ownMasterInstance).concatWith(Observable.never());
    }
}
