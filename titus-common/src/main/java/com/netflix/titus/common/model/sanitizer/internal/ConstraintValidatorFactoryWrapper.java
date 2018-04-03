/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.model.sanitizer.internal;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorFactory;

import com.netflix.titus.common.model.sanitizer.VerifierMode;
import org.hibernate.validator.internal.engine.constraintvalidation.ConstraintValidatorFactoryImpl;
import org.springframework.expression.EvaluationContext;

/**
 */
public class ConstraintValidatorFactoryWrapper implements ConstraintValidatorFactory {

    private final ConstraintValidatorFactoryImpl delegate;
    private final VerifierMode verifierMode;
    private final Function<Class<?>, Optional<ConstraintValidator<?, ?>>> applicationConstraintValidatorFactory;
    private final Supplier<EvaluationContext> spelContextFactory;

    public ConstraintValidatorFactoryWrapper(VerifierMode verifierMode,
                                             Function<Class<?>, Optional<ConstraintValidator<?, ?>>> applicationConstraintValidatorFactory,
                                             Supplier<EvaluationContext> spelContextFactory) {
        this.verifierMode = verifierMode;
        this.applicationConstraintValidatorFactory = applicationConstraintValidatorFactory;
        this.spelContextFactory = spelContextFactory;
        this.delegate = new ConstraintValidatorFactoryImpl();
    }

    @Override
    public <T extends ConstraintValidator<?, ?>> T getInstance(Class<T> key) {
        if (key == SpELClassValidator.class) {
            return (T) new SpELClassValidator(verifierMode, spelContextFactory);
        }
        if (key == SpELFieldValidator.class) {
            return (T) new SpELFieldValidator(verifierMode, spelContextFactory);
        }
        ConstraintValidator<?, ?> instance = applicationConstraintValidatorFactory.apply(key).orElseGet(() -> delegate.getInstance(key));
        return (T) instance;
    }

    @Override
    public void releaseInstance(ConstraintValidator<?, ?> instance) {
        if (instance instanceof SpELFieldValidator || instance instanceof SpELClassValidator) {
            return;
        }
        delegate.releaseInstance(instance);
    }
}
