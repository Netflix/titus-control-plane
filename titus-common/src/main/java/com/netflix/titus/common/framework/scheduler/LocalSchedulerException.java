package com.netflix.titus.common.framework.scheduler;

public class LocalSchedulerException extends RuntimeException {

    public enum ErrorCode {
        NotFound
    }

    private final ErrorCode errorCode;

    private LocalSchedulerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static LocalSchedulerException scheduleNotFound(String scheduleId) {
        return new LocalSchedulerException(ErrorCode.NotFound, String.format("Schedule not found: id=%s", scheduleId));
    }
}
