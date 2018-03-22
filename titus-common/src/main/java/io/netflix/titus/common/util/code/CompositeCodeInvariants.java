package io.netflix.titus.common.util.code;

import java.util.List;

import static java.util.Arrays.asList;

public class CompositeCodeInvariants extends CodeInvariants {

    private final List<CodeInvariants> codeInvariants;

    public CompositeCodeInvariants(CodeInvariants... codeInvariants) {
        this(asList(codeInvariants));
    }

    public CompositeCodeInvariants(List<CodeInvariants> codeInvariants) {
        this.codeInvariants = codeInvariants;
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        codeInvariants.forEach(i -> i.isTrue(condition, message, args));
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        codeInvariants.forEach(i -> i.notNull(value, message, args));
        return this;
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        codeInvariants.forEach(i -> i.inconsistent(message, args));
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        codeInvariants.forEach(i -> i.unexpectedError(message, e));
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        codeInvariants.forEach(i -> i.unexpectedError(message, args));
        return this;
    }
}
