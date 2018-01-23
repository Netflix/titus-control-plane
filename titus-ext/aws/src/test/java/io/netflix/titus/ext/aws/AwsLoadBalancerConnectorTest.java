/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.ext.aws;

import java.util.List;
import java.util.Set;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsyncClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroupNotFoundException;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.connector.cloud.CloudConnectorException;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.ext.aws.loadbalancer.AwsLoadBalancerConnector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.observers.TestSubscriber;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Tests the AWS load balancer connector. These tests should be DISABLED by default since they require
 * AWS credentials to work and are thus not portable.
 * The AWS resources to test are expected to have been created already and their info is consumed via
 * local credential files.
 */
public class AwsLoadBalancerConnectorTest {
    private static Logger logger = LoggerFactory.getLogger(LoadBalancerConnector.class);

    private static final String REGION = "us-east-1";
    private LoadBalancerConnector awsLoadBalancerConnector;

    private final String validIpTargetGroup = System.getenv("validTargetGroup");
    private final String invalidIpTargetGroup = System.getenv("invalidTargetGroup");
    private final String nonExistentTarget = System.getenv("nonExistentTargetGroup");
    private final String targetGroupWithTargets = System.getenv("targetGroupWithTargets");

    @Before
    public void setUp() throws Exception {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        AmazonElasticLoadBalancingAsync albClient = AmazonElasticLoadBalancingAsyncClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(REGION)
                .build();
        awsLoadBalancerConnector = new AwsLoadBalancerConnector(albClient, new DefaultRegistry());
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateIpTargetGroupTest() throws Exception {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(validIpTargetGroup).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateInstanceTargetGroupTest() throws Exception {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(invalidIpTargetGroup).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(CloudConnectorException.class);
    }

    @Ignore("AWS dependencies")
    @Test
    public void validateNonExistentTargetGroupTest() throws Exception {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.isValid(nonExistentTarget).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(TargetGroupNotFoundException.class);
    }

    @Ignore("AWS dependencies")
    @Test
    public void testGetIpTargets() throws Exception {
        TestSubscriber testSubscriber = new TestSubscriber();
        awsLoadBalancerConnector.getRegisteredIps(targetGroupWithTargets).subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        Set<String> targetSet = ((List<Set<String>>)testSubscriber.getOnNextEvents()).get(0);
        assertThat(targetSet.size()).isGreaterThan(0);
    }
}
