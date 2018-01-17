package io.netflix.titus.ext.cassandra.tool;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.StringExt;

/**
 * A collection of invariant predicates/enforcers.
 */
public final class StorePreconditions {

    private static Pattern[] DEV_STACK_PATTERNS = new Pattern[]{
            Pattern.compile(".*devvpc.*"),
            Pattern.compile(".*(test|Testing).*"),
            Pattern.compile(".*backup.*"),
            Pattern.compile(".*BACKUP.*"),
    };

    private StorePreconditions() {
    }

    /**
     * Check if keyspace belongs to development or test stack. Certain commands are destructive, and should be never
     * allowed on production stacks.
     */
    public static boolean isDevOrBackupStack(String keySpaceName) {
        Preconditions.checkArgument(StringExt.isNotEmpty(keySpaceName), "Expected keyspace name not null");

        for (Pattern p : DEV_STACK_PATTERNS) {
            if (p.matcher(keySpaceName).matches()) {
                return true;
            }
        }
        return false;
    }
}
