package com.netflix.titus.ext.aws;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.arn.Arn;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsync;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingAsyncClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsync;

@Singleton
public class AmazonClientProvider {

    private final AwsConfiguration configuration;
    private final AWSSecurityTokenServiceAsync stsClient;

    private ConcurrentMap<String, AWSCredentialsProvider> awsCredentialsByAccountId = new ConcurrentHashMap<>();
    private ConcurrentMap<String, AmazonElasticLoadBalancingAsync> loadBalancerClients = new ConcurrentHashMap<>();

    @Inject
    public AmazonClientProvider(AwsConfiguration configuration,
                                AWSSecurityTokenServiceAsync stsClient) {
        this.configuration = configuration;
        this.stsClient = stsClient;
    }

    public AmazonElasticLoadBalancingAsync getLoadBalancingClient(String accountId) {
        return loadBalancerClients.computeIfAbsent(accountId, id -> {
            String region = configuration.getRegion().trim().toLowerCase();
            AWSCredentialsProvider credentialsProvider = getAwsCredentialsProvider(id);
            return AmazonElasticLoadBalancingAsyncClientBuilder.standard()
                    .withCredentials(credentialsProvider)
                    .withRegion(region)
                    .build();
        });
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(String accountId) {
        return awsCredentialsByAccountId.computeIfAbsent(accountId, id -> {
            String roleSessionName = configuration.getControlPlaneRoleSessionName();
            int roleSessionDurationSeconds = configuration.getControlPlaneRoleSessionDurationSeconds();
            Arn roleArn = getControlPlaneRoleArnForAccount(id);

            return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn.toString(), roleSessionName)
                    .withStsClient(stsClient)
                    .withRoleSessionDurationSeconds(roleSessionDurationSeconds)
                    .build();
        });
    }

    private Arn getControlPlaneRoleArnForAccount(String accountId) {
        String resource = "role/" + configuration.getControlPlaneRoleName();
        return Arn.builder()
                .withPartition("aws")
                .withService("iam")
                .withRegion("")
                .withAccountId(accountId)
                .withResource(resource)
                .build();
    }
}
