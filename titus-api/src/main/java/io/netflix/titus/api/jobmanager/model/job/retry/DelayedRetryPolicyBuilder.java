package io.netflix.titus.api.jobmanager.model.job.retry;

import java.util.concurrent.TimeUnit;

public final class DelayedRetryPolicyBuilder extends RetryPolicy.RetryPolicyBuilder<DelayedRetryPolicy,DelayedRetryPolicyBuilder> {

    private long delayMs;

    DelayedRetryPolicyBuilder() {
    }

    public DelayedRetryPolicyBuilder withDelay(long delay, TimeUnit timeUnit) {
        this.delayMs = timeUnit.toMillis(delay);
        return this;
    }

    public DelayedRetryPolicy build() {
        return new DelayedRetryPolicy(delayMs, retries);
    }
}
