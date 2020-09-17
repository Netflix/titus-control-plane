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
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.aws.SpectatorRequestMetricCollector;
import com.netflix.titus.common.runtime.TitusRuntime;

@Singleton
public class AmazonClientProvider {

    private final AwsConfiguration configuration;
    private final AWSSecurityTokenServiceAsync stsClient;

    private final Map<String, AWSCredentialsProvider> awsCredentialsByAccountId = new HashMap<>();
    private final Map<String, AmazonElasticLoadBalancingAsync> loadBalancerClients = new HashMap<>();
    private final Registry registry;

    @Inject
    public AmazonClientProvider(AwsConfiguration configuration,
                                AWSSecurityTokenServiceAsync stsClient,
                                TitusRuntime runtime) {
        this.configuration = configuration;
        this.stsClient = stsClient;
        this.registry = runtime.getRegistry();
    }

    public AmazonElasticLoadBalancingAsync getLoadBalancingClient(String accountId) {
        AmazonElasticLoadBalancingAsync client = loadBalancerClients.get(accountId);
        if (client == null) {
            synchronized (this) {
                client = loadBalancerClients.get(accountId);
                if (client == null) {
                    String region = AwsRegionConfigurationUtil.resolveDataPlaneRegion(configuration);
                    AWSCredentialsProvider credentialsProvider = getAwsCredentialsProvider(accountId);
                    client = AmazonElasticLoadBalancingAsyncClientBuilder.standard()
                            .withCredentials(credentialsProvider)
                            .withRegion(region)
                            .withMetricsCollector(new SpectatorRequestMetricCollector(registry))
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
            synchronized (this) {
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
