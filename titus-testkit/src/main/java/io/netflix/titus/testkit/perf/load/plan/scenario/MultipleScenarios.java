package io.netflix.titus.testkit.perf.load.plan.scenario;

import java.util.List;
import java.util.stream.Collectors;

import io.netflix.titus.testkit.perf.load.plan.ExecutionScenario;
import rx.Observable;

public class MultipleScenarios extends ExecutionScenario {

    private final Observable<Executable> mergedPlans;
    private final List<ExecutionScenario> executionScenarios;

    public MultipleScenarios(List<ExecutionScenario> executionScenarios) {
        this.mergedPlans = Observable.merge(
                executionScenarios.stream().map(ExecutionScenario::executionPlans).collect(Collectors.toList())
        );
        this.executionScenarios = executionScenarios;
    }

    @Override
    public Observable<Executable> executionPlans() {
        return mergedPlans;
    }

    @Override
    public void completed(Executable executable) {
        executionScenarios.forEach(s -> s.completed(executable));
    }
}
