package com.netflix.titus.master.supervisor.model;

public enum MasterState {
    Starting,
    Inactive,
    NonLeader,
    LeaderActivating,
    LeaderActivated;

    public static boolean isLeader(MasterState state) {
        return state == MasterState.LeaderActivating || state == MasterState.LeaderActivated;
    }

    public static boolean isAfter(MasterState before, MasterState after) {
        return before.ordinal() < after.ordinal();
    }
}
