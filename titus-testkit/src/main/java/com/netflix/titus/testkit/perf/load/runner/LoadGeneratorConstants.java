package com.netflix.titus.testkit.perf.load.runner;

import com.netflix.titus.api.model.callmetadata.CallMetadata;

public final class LoadGeneratorConstants {
    /**
     * Call metadata for cleanup actions between test runs
     */
    public static final CallMetadata JOB_TERMINATOR_CALL_METADATA = CallMetadata.newBuilder().withCallerId("JobTerminator").withCallReason("Cleanup").build();
    /**
     * Call metadata for test runs
     */
    public static final CallMetadata TEST_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Test").withCallReason("Test").build();
}
