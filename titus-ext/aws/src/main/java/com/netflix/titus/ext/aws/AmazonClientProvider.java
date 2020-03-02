package com.netflix.titus.ext.aws;

import java.util.HashMap;
import java.util.Map;
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

    private Map<String, AWSCredentialsProvider> awsCredentialsByAccountId = new HashMap<>();
    private Map<String, AmazonElasticLoadBalancingAsync> loadBalancerClients = new HashMap<>();

    @Inject
    public AmazonClientProvider(AwsConfiguration configuration,
                                AWSSecurityTokenServiceAsync stsClient) {
        this.configuration = configuration;
        this.stsClient = stsClient;
    }

    public AmazonElasticLoadBalancingAsync getLoadBalancingClient(String accountId) {
        AmazonElasticLoadBalancingAsync client = loadBalancerClients.get(accountId);
        if (client == null) {
            synchronized (AmazonClientProvider.class) {
                client = loadBalancerClients.get(accountId);
                if (client == null) {
                    String region = configuration.getRegion().trim().toLowerCase();
                    AWSCredentialsProvider credentialsProvider = getAwsCredentialsProvider(accountId);
                    client = AmazonElasticLoadBalancingAsyncClientBuilder.standard()
                            .withCredentials(credentialsProvider)
                            .withRegion(region)
                            .build();
                    loadBalancerClients.put(accountId, client);
                }
            }
        }
        return client;
    }

    private AWSCredentialsProvider getAwsCredentialsProvider(String accountId) {
        AWSCredentialsProvider credentialsProvider = awsCredentialsByAccountId.get(accountId);
        if (credentialsProvider == null) {
            synchronized (AmazonClientProvider.class) {
                credentialsProvider = awsCredentialsByAccountId.get(accountId);
                if (credentialsProvider == null) {
                    String roleSessionName = configuration.getControlPlaneRoleSessionName();
                    int roleSessionDurationSeconds = configuration.getControlPlaneRoleSessionDurationSeconds();
                    Arn roleArn = getControlPlaneRoleArnForAccount(accountId);

                    credentialsProvider = new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn.toString(), roleSessionName)
                            .withStsClient(stsClient)
                            .withRoleSessionDurationSeconds(roleSessionDurationSeconds)
                            .build();
                    awsCredentialsByAccountId.put(accountId, credentialsProvider);
                }
            }
        }
        return credentialsProvider;
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
