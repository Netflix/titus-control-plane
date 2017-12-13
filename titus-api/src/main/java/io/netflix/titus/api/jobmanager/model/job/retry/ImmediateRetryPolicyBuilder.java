package io.netflix.titus.api.jobmanager.model.job.retry;

public class ImmediateRetryPolicyBuilder extends RetryPolicy.RetryPolicyBuilder<ImmediateRetryPolicy, ImmediateRetryPolicyBuilder> {

    @Override
    public ImmediateRetryPolicy build() {
        return new ImmediateRetryPolicy(retries);
    }
}
