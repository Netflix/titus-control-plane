package io.netflix.titus.common.util.code;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodeInvariants {

    private static final Logger logger = LoggerFactory.getLogger(CodeInvariants.class);

    private static CodeInvariants INSTANCE = new CodeInvariants();

    public CodeInvariants isTrue(boolean condition, String message, Object... args) {
        if (!condition) {
            inconsistent(message, args);
        }
        return this;
    }

    public CodeInvariants notNull(Object value, String message, Object... args) {
        if (value == null) {
            inconsistent(message, args);
        }
        return this;
    }

    public CodeInvariants inconsistent(String message, Object... args) {
        if (args.length == 0) {
            logger.warn(message);
        }

        try {
            logger.warn(String.format(message, args));
        } catch (Exception e) {
            logger.warn(message + " (" + e.getMessage() + ')');
        }

        return this;
    }

    public void unexpectedError(String message, Exception e) {
        logger.warn(message, e);
    }

    public static CodeInvariants codeInvariants() {
        return INSTANCE;
    }
}
