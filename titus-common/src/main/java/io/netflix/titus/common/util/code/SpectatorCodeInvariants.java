package io.netflix.titus.common.util.code;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class SpectatorCodeInvariants extends CodeInvariants {

    private final Counter violations;

    public SpectatorCodeInvariants(Id rootId, Registry registry) {
        this.violations = registry.counter(rootId);
    }

    @Override
    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            violations.increment();
        }
        return this;
    }

    @Override
    public CodeInvariants notNull(Object value, String message, Object... args) {
        if (value == null) {
            violations.increment();
        }
        return this;
    }

    @Override
    public CodeInvariants inconsistent(String message, Object... args) {
        violations.increment();
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Exception e) {
        violations.increment();
        return this;
    }

    @Override
    public CodeInvariants unexpectedError(String message, Object... args) {
        violations.increment();
        return this;
    }
}
