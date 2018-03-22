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

package io.netflix.titus.common.model.sanitizer.internal;

import java.util.function.Supplier;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import io.netflix.titus.common.model.sanitizer.FieldInvariant;
import io.netflix.titus.common.model.sanitizer.VerifierMode;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class SpELFieldValidator implements ConstraintValidator<FieldInvariant, Object> {

    private final ExpressionParser parser = new SpelExpressionParser();
    private final VerifierMode verifierMode;
    private final Supplier<EvaluationContext> spelContextFactory;

    private boolean enabled;
    private Expression expression;
    private EvaluationContext spelContext;

    public SpELFieldValidator(VerifierMode verifierMode, Supplier<EvaluationContext> spelContextFactory) {
        this.verifierMode = verifierMode;
        this.spelContextFactory = spelContextFactory;
    }

    @Override
    public void initialize(FieldInvariant constraintAnnotation) {
        this.enabled = verifierMode.includes(constraintAnnotation.mode());
        if (enabled) {
            this.expression = parser.parseExpression(constraintAnnotation.value());
            this.spelContext = spelContextFactory.get();
        }
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        if (!enabled) {
            return true;
        }
        return (boolean) expression.getValue(spelContext, new Root(value));
    }

    public static class Root {
        private final Object value;

        public Root(Object value) {
            this.value = value;
        }

        public Object getValue() {
            return value;
        }
    }
}
