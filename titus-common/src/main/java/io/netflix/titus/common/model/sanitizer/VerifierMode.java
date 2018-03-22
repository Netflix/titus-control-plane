package io.netflix.titus.common.model.sanitizer;

/**
 * Enum type for {@link EntitySanitizer} validation modes. Each {@link EntitySanitizer} instance runs either
 * in strict or permissive mode. Each {@link ClassInvariant}, and {@link FieldInvariant} validation rule is
 * configured with the validation mode as well, with the permissive mode as a default. For example,
 * if the {@link EntitySanitizer} is configured as permissive, and there is a filed with {@link ClassInvariant} annotation
 * with the {@link #Strict} mode, it will not be checked during the validation process. It would be however checked
 * if the {@link EntitySanitizer} was in the {@link #Strict} mode.
 */
public enum VerifierMode {
    /**
     * Most restrictive mode. Validation rules annotated as 'Strict' are only checked in the strict mode.
     */
    Strict {
        @Override
        public boolean includes(VerifierMode mode) {
            return true;
        }
    },

    /**
     * Default validation mode. Checks non strict validation rules only.
     */
    Permissive {
        @Override
        public boolean includes(VerifierMode mode) {
            return mode == Permissive;
        }
    };

    public abstract boolean includes(VerifierMode mode);
}
