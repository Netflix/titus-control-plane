package io.netflix.titus.runtime.endpoint.resolver;

/**
 * Resolves caller id from its host name or IP address.
 */
public interface HostCallerIdResolver extends CallerIdResolver<String> {
}
