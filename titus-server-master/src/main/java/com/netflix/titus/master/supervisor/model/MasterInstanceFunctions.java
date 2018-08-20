package com.netflix.titus.master.supervisor.model;

import com.netflix.titus.common.util.CollectionsExt;

public final class MasterInstanceFunctions {

    private MasterInstanceFunctions() {
    }

    public static MasterInstance moveTo(MasterInstance masterInstance, MasterStatus nextStatus) {
        return masterInstance.toBuilder()
                .withStatus(nextStatus)
                .withStatusHistory(CollectionsExt.copyAndAdd(masterInstance.getStatusHistory(), masterInstance.getStatus()))
                .build();
    }

    public static boolean areDifferent(MasterInstance first, MasterInstance second) {
        MasterInstance firstNoTimestamp = first.toBuilder().withStatus(first.getStatus().toBuilder().withTimestamp(0).build()).build();
        MasterInstance secondNoTimestamp = second.toBuilder().withStatus(second.getStatus().toBuilder().withTimestamp(0).build()).build();
        return !firstNoTimestamp.equals(secondNoTimestamp);
    }
}
