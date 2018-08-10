package com.netflix.titus.master.supervisor.service;

import com.netflix.titus.master.supervisor.model.MasterInstance;
import rx.Observable;

public interface LocalMasterInstanceResolver {

    Observable<MasterInstance> observeLocalMasterInstanceUpdates();
}
