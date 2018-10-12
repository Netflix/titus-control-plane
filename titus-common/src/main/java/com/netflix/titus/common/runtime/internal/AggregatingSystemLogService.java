package com.netflix.titus.common.runtime.internal;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.SystemLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AggregatingSystemLogService implements SystemLogService {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingSystemLogService.class);

    private final Set<SystemLogService> delegates;

    @Inject
    public AggregatingSystemLogService(Set<SystemLogService> delegates) {
        this.delegates = delegates;
    }

    @Override
    public boolean submit(SystemLogEvent event) {
        boolean result = true;
        for (SystemLogService delegate : delegates) {
            try {
                result = result && delegate.submit(event);
            } catch (Exception e) {
                logger.warn("Logging error in delegate {}: {}", delegate.getClass().getSimpleName(), e.getMessage());
                result = false;
            }
        }
        return result;
    }
}
