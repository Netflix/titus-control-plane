package io.netflix.titus.common.model.sanitizer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.validation.ConstraintViolation;

/**
 * Collection of helper functions.
 */
public final class EntitySanitizerUtil {

    private EntitySanitizerUtil() {
    }

    public static Map<String, String> toStringMap(Collection<ConstraintViolation<?>> violations) {
        if (violations == null) {
            return Collections.emptyMap();
        }
        Map<String, String> violationsMap = new HashMap<>();
        for (ConstraintViolation<?> violation : violations) {
            Object message = violation.getMessage();
            if (message != null) {
                violationsMap.put(violation.getPropertyPath().toString(), message.toString());
            }
        }
        return violationsMap;
    }
}
