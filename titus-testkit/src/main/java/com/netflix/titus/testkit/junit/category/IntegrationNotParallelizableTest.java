package com.netflix.titus.testkit.junit.category;

/**
 * Integration tests that cannot run in parallel should include this annotation (and only this one).
 */
public @interface IntegrationNotParallelizableTest {
}
