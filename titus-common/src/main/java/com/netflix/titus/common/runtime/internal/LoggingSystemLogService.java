package com.netflix.titus.common.runtime.internal;

import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.SystemLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSystemLogService implements SystemLogService {

    private static final Logger logger = LoggerFactory.getLogger(LoggingSystemLogService.class);

    private static final LoggingSystemLogService INSTANCE = new LoggingSystemLogService();

    public static LoggingSystemLogService getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean submit(SystemLogEvent event) {
        switch (event.getPriority()) {
            case Info:
                logger.info("System event: {}", event);
                break;
            case Warn:
                logger.warn("System event: {}", event);
                break;
            case Error:
            case Fatal:
                logger.error("System event: {}", event);
                break;
        }
        return true;
    }
}
