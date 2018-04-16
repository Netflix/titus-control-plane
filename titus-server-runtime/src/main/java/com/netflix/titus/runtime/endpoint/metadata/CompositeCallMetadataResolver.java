package com.netflix.titus.runtime.endpoint.metadata;

import java.util.List;
import java.util.Optional;

public class CompositeCallMetadataResolver implements CallMetadataResolver {

    private final List<CallMetadataResolver> resolvers;

    public CompositeCallMetadataResolver(List<CallMetadataResolver> resolvers) {
        this.resolvers = resolvers;
    }

    @Override
    public Optional<CallMetadata> resolve() {
        for (CallMetadataResolver resolver : resolvers) {
            Optional<CallMetadata> next = resolver.resolve();
            if (next.isPresent()) {
                return next;
            }
        }
        return Optional.empty();
    }
}
