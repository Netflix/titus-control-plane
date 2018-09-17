package com.netflix.titus.common.model.validator;

import java.util.Set;

/**
 * A Validator determines whether an object of the parameterized type is valid.  If it finds an object to be invalid it
 * returns a non-empty set of {@link ValidationError}s.
 *
 * @param <T> The type of object this Validator validates.
 */
public interface Validator<T> {
    Set<ValidationError> validate(T entity);
}
