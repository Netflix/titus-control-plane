package com.netflix.titus.common.runtime.internal;

import com.netflix.titus.common.runtime.SystemLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingSystemLogService implements SystemLogService {

    private static final Logger logger = LoggerFactory.getLogger(LoggingSystemLogService.class);

    private static final LoggingSystemLogService INSTANCE = new LoggingSystemLogService();

    @Override
    public boolean write(Category category, String priority, String component, String message) {
        logger.warn("[{}] [{}] {}: {}", category, component, priority, message);
        return true;
    }

    public static LoggingSystemLogService getInstance() {
        return INSTANCE;
    }
}
