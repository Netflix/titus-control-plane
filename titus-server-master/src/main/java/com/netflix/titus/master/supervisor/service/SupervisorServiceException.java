package com.netflix.titus.master.supervisor.service;

import com.netflix.titus.master.supervisor.model.MasterInstance;

import static java.lang.String.format;

public class SupervisorServiceException extends RuntimeException {

    public enum ErrorCode {
        MasterInstanceNotFound,
        NotLeader,
    }

    private final ErrorCode errorCode;

    private SupervisorServiceException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    private SupervisorServiceException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static SupervisorServiceException masterInstanceNotFound(String instanceId) {
        return new SupervisorServiceException(ErrorCode.MasterInstanceNotFound, format("TitusMaster instance with id %s does not exist", instanceId));
    }

    public static SupervisorServiceException notLeader(MasterInstance currentMasterInstance) {
        return new SupervisorServiceException(ErrorCode.NotLeader, format("TitusMaster instance %s is not a leader", currentMasterInstance.getInstanceId()));
    }
}
