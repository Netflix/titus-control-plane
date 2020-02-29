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

import java.util.Map;
import java.util.function.Supplier;

import com.netflix.titus.common.model.sanitizer.ClassInvariant;
import com.netflix.titus.common.model.sanitizer.VerifierMode;
import com.netflix.titus.common.util.CollectionsExt;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * Spring EL JavaBean validation framework class-level validator.
 */
public class SpELClassValidator extends AbstractConstraintValidator<ClassInvariant, Object> {

    private final ExpressionParser parser = new SpelExpressionParser();
    private final VerifierMode verifierMode;
    private final Supplier<EvaluationContext> spelContextFactory;

    private boolean enabled;
    private Expression conditionExpression;
    private Expression exprExpression;
    private EvaluationContext spelContext;

    public SpELClassValidator(VerifierMode verifierMode, Supplier<EvaluationContext> spelContextFactory) {
        this.verifierMode = verifierMode;
        this.spelContextFactory = spelContextFactory;
    }

    @Override
    public void initialize(ClassInvariant constraintAnnotation) {
        this.enabled = verifierMode.includes(constraintAnnotation.mode());
        if (enabled) {
            if (!constraintAnnotation.condition().isEmpty()) {
                this.conditionExpression = parser.parseExpression(constraintAnnotation.condition());
            } else if (!constraintAnnotation.expr().isEmpty()) {
                this.exprExpression = parser.parseExpression(constraintAnnotation.expr());
            }
            this.spelContext = spelContextFactory.get();
        }
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContextWrapper context) {
        if (!enabled) {
            return true;
        }
        if (conditionExpression != null) {
            return (boolean) conditionExpression.getValue(spelContext, value);
        }

        Map<String, String> violations = (Map<String, String>) exprExpression.getValue(spelContext, value);
        if (CollectionsExt.isNullOrEmpty(violations)) {
            return true;
        }

        violations.forEach((field, message) -> context.buildConstraintViolationWithStaticMessage(message)
                .addPropertyNode(field)
                .addConstraintViolation()
                .disableDefaultConstraintViolation());

        return false;
    }
}
