package io.netflix.titus.common.model.sanitizer;

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
