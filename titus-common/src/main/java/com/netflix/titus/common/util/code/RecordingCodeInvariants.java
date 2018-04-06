package com.netflix.titus.common.util.code;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.netflix.titus.common.util.ExceptionExt.doTry;

public class RecordingCodeInvariants extends CodeInvariants {

    private final List<String> violations = new CopyOnWriteArrayList<>();

    public List<String> getViolations() {
        return new ArrayList<>(violations);
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            violations.add(doTry(() -> String.format(message, args)).orElse("Invalid pattern or arguments: " + message));
        }
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        return isTrue(value != null, message, args);
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        return isTrue(false, message, args);
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        return isTrue(false, message);
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        return isTrue(false, message, args);
    }
}
