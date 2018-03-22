/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.common.model.sanitizer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.validation.Validation;
import javax.validation.Validator;

import io.netflix.titus.common.model.sanitizer.internal.ConstraintValidatorFactoryWrapper;
import io.netflix.titus.common.model.sanitizer.internal.SpELMessageInterpolator;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import static java.util.Collections.singletonMap;

/**
 * Pre-configured JavaBean validators with Titus validation extensions, used by unit tests.
 */
public final class TestValidator {

    public static Validator testStrictValidator() {
        return testValidator(VerifierMode.Strict);
    }

    public static Validator testPermissiveValidator() {
        return testValidator(VerifierMode.Permissive);
    }

    public static Validator testValidator(VerifierMode verifierMode) {
        TestModel.setFit(true);

        Map<String, Object> registeredObjects = singletonMap("env", System.getProperties());

        Supplier<EvaluationContext> spelContextFactory = () -> {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.registerFunction("fit", TestModel.getFitMethod());
            context.setBeanResolver((ctx, beanName) -> registeredObjects.get(beanName));
            return context;
        };

        return Validation.buildDefaultValidatorFactory()
                .usingContext()
                .constraintValidatorFactory(new ConstraintValidatorFactoryWrapper(verifierMode, type -> Optional.empty(), spelContextFactory))
                .messageInterpolator(new SpELMessageInterpolator(spelContextFactory))
                .getValidator();
    }

    public static Validator testValidator(String alias, Object bean) {
        Supplier<EvaluationContext> spelContextFactory = () -> {
            StandardEvaluationContext context = new StandardEvaluationContext();
            context.setBeanResolver((ctx, beanName) -> beanName.equals(alias) ? bean : null);
            context.setMethodResolvers(Collections.singletonList(new ReflectiveMethodResolver()));
            return context;
        };

        return Validation.buildDefaultValidatorFactory()
                .usingContext()
                .constraintValidatorFactory(new ConstraintValidatorFactoryWrapper(VerifierMode.Strict, type -> Optional.empty(), spelContextFactory))
                .messageInterpolator(new SpELMessageInterpolator(spelContextFactory))
                .getValidator();
    }
}
