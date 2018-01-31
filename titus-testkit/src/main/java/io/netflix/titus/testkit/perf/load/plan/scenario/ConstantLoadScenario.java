package io.netflix.titus.testkit.perf.load.plan.scenario;

import com.google.common.base.Preconditions;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import io.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Observable;
import rx.Subscriber;
import rx.observers.SerializedSubscriber;

public class ConstantLoadScenario extends ExecutionScenario {

    private final Executable executable;
    private final int size;

    private volatile Subscriber<? super Executable> scenarioSubscriber;

    public ConstantLoadScenario(String owner, JobDescriptor<?> jobSpec, ExecutionPlan plan, int size) {
        this.executable = new Executable(owner, jobSpec, plan);
        this.size = size;
    }

    @Override
    public Observable<Executable> executionPlans() {
        return Observable.unsafeCreate(subscriber -> {
            // FIXME This is prone to race conditions.
            Preconditions.checkState(scenarioSubscriber == null, "Expected single subscription");
            scenarioSubscriber = new SerializedSubscriber<>(subscriber);
            for (int i = 0; i < size; i++) {
                subscriber.onNext(executable);
            }
        });
    }

    @Override
    public void completed(Executable executable) {
        if (scenarioSubscriber != null && executable == this.executable) {
            scenarioSubscriber.onNext(executable);
        }
    }
}
