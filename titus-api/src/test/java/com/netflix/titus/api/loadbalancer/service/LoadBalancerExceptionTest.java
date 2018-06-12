package com.netflix.titus.api.loadbalancer.service;

import org.junit.Test;
import org.slf4j.event.Level;

import static org.junit.Assert.assertEquals;

/**
 * Tests the {@link LoadBalancerException} class.
 */
public class LoadBalancerExceptionTest {
    @Test
    public void getDefaultLogLevelTest() {
        assertEquals(Level.ERROR, LoadBalancerException.getLogLevel(new Exception()));
    }

    @Test
    public void getDefaultLoadBalancerExceptionLogLevelTest() {
        assertEquals(Level.ERROR, LoadBalancerException.getLogLevel(LoadBalancerException.jobNotRoutableIp("job-id")));
    }

    @Test
    public void getTaskGroupNotFoundLogLevelTest() {
        assertEquals(
                Level.DEBUG,
                LoadBalancerException.getLogLevel(
                        LoadBalancerException.targetGroupNotFound("target-group-id", new Exception())));
    }
}
