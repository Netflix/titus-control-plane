package com.netflix.titus.master.eviction.service.quota.system;

import com.netflix.titus.master.eviction.service.quota.ConsumptionResult;
import com.netflix.titus.runtime.connector.eviction.EvictionRejectionReasons;

public class SystemQuotaConsumptionResults {
    public static final ConsumptionResult QUOTA_LIMIT_EXCEEDED = ConsumptionResult.rejected(EvictionRejectionReasons.LIMIT_EXCEEDED.getReasonMessage());
    public static final ConsumptionResult OUTSIDE_SYSTEM_TIME_WINDOW = ConsumptionResult.rejected(EvictionRejectionReasons.SYSTEM_WINDOW_CLOSED.getReasonMessage());
}
