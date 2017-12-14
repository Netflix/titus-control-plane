package io.netflix.titus.api.jobmanager.model.job.retry;

public final class ExponentialBackoffRetryPolicyBuilder extends RetryPolicy.RetryPolicyBuilder<ExponentialBackoffRetryPolicy, ExponentialBackoffRetryPolicyBuilder> {

    private long initialDelayMs;
    private long maxDelayMs;

    ExponentialBackoffRetryPolicyBuilder() {
    }

    public ExponentialBackoffRetryPolicyBuilder withInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
        return this;
    }

    public ExponentialBackoffRetryPolicyBuilder withMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
        return this;
    }

    public ExponentialBackoffRetryPolicyBuilder but() {
        return ExponentialBackoffRetryPolicy.newBuilder().withInitialDelayMs(initialDelayMs).withMaxDelayMs(maxDelayMs).withRetries(retries);
    }

    public ExponentialBackoffRetryPolicy build() {
        return new ExponentialBackoffRetryPolicy(retries, initialDelayMs, maxDelayMs);
    }
}
