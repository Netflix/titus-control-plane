package io.netflix.titus.master.scheduler;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;

/**
 * Transforms V2/V3 engine specific constraint model to Fenzo model.
 */
public interface ConstraintEvaluatorTransformer<CONSTRAINT> {

    Optional<ConstraintEvaluator> hardConstraint(CONSTRAINT constraint, Supplier<Set<String>> activeTasksGetter);

    Optional<VMTaskFitnessCalculator> softConstraint(CONSTRAINT constraint, Supplier<Set<String>> activeTasksGetter);
}
