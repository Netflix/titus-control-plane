package com.netflix.titus.master.supervisor.service;

import static java.lang.String.format;

public class SupervisorServiceException extends RuntimeException {

    public enum ErrorCode {
        MasterInstanceNotFound,
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
}
