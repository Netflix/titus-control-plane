package com.netflix.titus.api.eviction.model;

import com.netflix.titus.api.model.Reference;
import com.netflix.titus.api.model.TokenBucketPolicy;

public class SystemDisruptionBudget {

    private final Reference reference;
    private final TokenBucketPolicy tokenBucketPolicy;

    public SystemDisruptionBudget(Reference reference, TokenBucketPolicy tokenBucketPolicy) {
        this.reference = reference;
        this.tokenBucketPolicy = tokenBucketPolicy;
    }

    public Reference getReference() {
        return reference;
    }

    public TokenBucketPolicy getTokenBucketPolicy() {
        return tokenBucketPolicy;
    }

    public Builder toBuilder() {
        return newBuilder().withReference(reference).withTokenBucketDescriptor(tokenBucketPolicy);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Reference reference;
        private TokenBucketPolicy tokenBucketPolicy;

        private Builder() {
        }

        public Builder withReference(Reference reference) {
            this.reference = reference;
            return this;
        }

        public Builder withTokenBucketDescriptor(TokenBucketPolicy tokenBucketPolicy) {
            this.tokenBucketPolicy = tokenBucketPolicy;
            return this;
        }

        public SystemDisruptionBudget build() {
            return new SystemDisruptionBudget(reference, tokenBucketPolicy);
        }
    }
}
