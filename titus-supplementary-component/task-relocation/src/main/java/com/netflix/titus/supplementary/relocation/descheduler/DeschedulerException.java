package com.netflix.titus.supplementary.relocation.descheduler;

public class DeschedulerException extends RuntimeException {

    public enum ErrorCode {
        NoQuotaLeft
    }

    private final ErrorCode errorCode;

    private DeschedulerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static DeschedulerException noQuotaLeft(String message, Object... args) {
        String formatted = args.length > 0
                ? String.format(message, args)
                : message;
        return new DeschedulerException(ErrorCode.NoQuotaLeft, formatted);
    }
}
