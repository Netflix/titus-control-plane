package com.netflix.titus.supplementary.relocation.workflow;

public class RelocationWorkflowException extends RuntimeException {

    public enum ErrorCode {
        StoreError
    }

    private final ErrorCode errorCode;

    private RelocationWorkflowException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static RelocationWorkflowException storeError(String message, Throwable cause) {
        return new RelocationWorkflowException(ErrorCode.StoreError, message, cause);
    }
}
