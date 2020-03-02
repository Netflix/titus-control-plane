/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.ext.aws;

import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsyncClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroupAssociationLimitException;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroupNotFoundException;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.connector.cloud.CloudConnectorException;
import com.netflix.titus.api.connector.cloud.LoadBalancer;
import com.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerException;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.ext.aws.loadbalancer.AwsLoadBalancerConnector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import rx.observers.TestSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the AWS load balancer connector. Those tests which require AWS credentials (and are thus not portable) should
 * be DISABLED.
 * <p>
 * In order to execute the disabled tests, the AWS resources to test are expected to have been created already and their
 * info is consumed via local credential files.
 */
public class AwsLoadBalancerConnectorTest {
    private static final String REGION = "us-east-1";
    private static final String DEFAULT_ARN = "arn:aws:elasticloadbalancing:us-east-2:123456789012:loadbalancer/app/my-load-balancer/1234567890123456";
    private LoadBalancerConnector awsLoadBalancerConnector;

    private final String validIpTargetGroup = StringExt.getNonEmptyOrDefault(System.getenv("validTargetGroup"), DEFAULT_ARN);
    private final String invalidIpTargetGroup = StringExt.getNonEmptyOrDefault(System.getenv("invalidTargetGroup"), DEFAULT_ARN);
    private final String nonExistentTarget = StringExt.getNonEmptyOrDefault(System.getenv("nonExistentTargetGroup"), DEFAULT_ARN);
    private final String targetGroupWithTargets = StringExt.getNonEmptyOrDefault(System.getenv("targetGroupWithTargets"), DEFAULT_ARN);

    @Before
    public void setUp() {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonElasticLoadBalancingAsync albClient = AmazonElasticLoadBalancingAsyncClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        awsLoadBalancerConnector = getAwsLoadBalancerConnector(albClient);
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateIpTargetGroupTest() {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(validIpTargetGroup).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateInstanceTargetGroupTest() {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(invalidIpTargetGroup).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(CloudConnectorException.class);
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateNonExistentTargetGroupTest() {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(nonExistentTarget).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(TargetGroupNotFoundException.class);
    }

    @Ignore("AWS dependencies")
    @Test
    public void testGetIpTargets() {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.getLoadBalancer(targetGroupWithTargets).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        LoadBalancer loadBalancer = (LoadBalancer) testSubscriber.getOnNextEvents().get(0);
        assertEquals(targetGroupWithTargets, loadBalancer.getId());
        assertThat(loadBalancer.getRegisteredIps()).isNotEmpty();
    }

    @Test
    public void validateTargetGroupNotFoundExceptionIsTranslatedToRemovedState() {
        TestSubscriber testSubscriber = new TestSubscriber();

        AmazonElasticLoadBalancingAsync albClient = mock(AmazonElasticLoadBalancingAsync.class);
        when(albClient.describeTargetHealthAsync(any(), any())).thenThrow(TargetGroupNotFoundException.class);

        awsLoadBalancerConnector = getAwsLoadBalancerConnector(albClient);
        awsLoadBalancerConnector.getLoadBalancer(targetGroupWithTargets).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        LoadBalancer loadBalancer = (LoadBalancer) testSubscriber.getOnNextEvents().get(0);
        assertEquals(targetGroupWithTargets, loadBalancer.getId());
        assertEquals(LoadBalancer.State.REMOVED, loadBalancer.getState());
    }

    @Test
    public void validateExceptionsAreUnmodifiedWithMockClientTest() {
        Class defaultExceptionClass = TargetGroupAssociationLimitException.class;
        TestSubscriber testSubscriber = new TestSubscriber();

        AmazonElasticLoadBalancingAsync albClient = mock(AmazonElasticLoadBalancingAsync.class);
        when(albClient.describeTargetHealthAsync(any(), any())).thenThrow(defaultExceptionClass);

        awsLoadBalancerConnector = getAwsLoadBalancerConnector(albClient);
        awsLoadBalancerConnector.getLoadBalancer(targetGroupWithTargets).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();

        List<Throwable> errors = testSubscriber.getOnErrorEvents();
        assertEquals(1, errors.size());

        Throwable throwable = errors.get(0);
        assertFalse(throwable instanceof LoadBalancerException);
        assertTrue(throwable instanceof TargetGroupAssociationLimitException);
    }

    private AwsLoadBalancerConnector getAwsLoadBalancerConnector(AmazonElasticLoadBalancingAsync albClient) {
        AmazonClientProvider amazonClientProvider = mock(AmazonClientProvider.class);
        when(amazonClientProvider.getLoadBalancingClient(any())).thenReturn(albClient);
        return new AwsLoadBalancerConnector(amazonClientProvider, new DefaultRegistry());
    }
}
