package io.netflix.titus.runtime.endpoint.resolver;

import java.util.Optional;
import javax.inject.Singleton;

/**
 * {@link HostCallerIdResolver} implementation that returns always an empty response.
 */
@Singleton
public class NoOpHostCallerIdResolver implements HostCallerIdResolver {
    @Override
    public Optional<String> resolve(String address) {
        return Optional.of("Anonymous");
    }
}
