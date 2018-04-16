package com.netflix.titus.runtime.endpoint.metadata;

import javax.inject.Provider;
import javax.inject.Singleton;

import static java.util.Arrays.asList;

@Singleton
public class SimpleCallMetadataResolverProvider implements Provider<CallMetadataResolver> {

    private final CompositeCallMetadataResolver resolver;

    public SimpleCallMetadataResolverProvider() {
        this.resolver = new CompositeCallMetadataResolver(
                asList(new SimpleGrpcCallMetadataResolver(), new SimpleHttpCallMetadataResolver())
        );
    }

    @Override
    public CallMetadataResolver get() {
        return resolver;
    }
}
