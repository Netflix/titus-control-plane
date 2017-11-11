package io.netflix.titus.runtime.endpoint.resolver;

import java.util.Optional;

/**
 * External client (caller) id resolver. The caller data type depends on actual transport protocol used.
 * It is also possible that for a single request, multiple caller identities exist.
 */
public interface CallerIdResolver<CALLER_DATA> {

    Optional<String> resolve(CALLER_DATA callerData);
}
