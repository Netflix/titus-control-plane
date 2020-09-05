package com.netflix.titus.supplementary.relocation.util;

import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class RelocationUtilTest {

    @Test
    public void buildTasksFromNodesAndJobsFilter() {
        ReadOnlyJobOperations jobOperations = mock(ReadOnlyJobOperations.class);
    }
}