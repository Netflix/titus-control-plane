package com.netflix.titus.ext.aws.apigateway;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.runtime.endpoint.common.rest.Responses;
import io.swagger.annotations.Api;
import io.swagger.jaxrs.PATCH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api(tags = "Auto scaling")
@Path("/v1/scalableTargetDimensions")
@Singleton
public class ApiGatewayCallbackResource {
    private static final Logger logger = LoggerFactory.getLogger(ApiGatewayCallbackResource.class);
    private AwsGatewayCallbackService awsGatewayCallbackService;

    public ApiGatewayCallbackResource(AwsGatewayCallbackService awsGatewayCallbackService) {
        this.awsGatewayCallbackService = awsGatewayCallbackService;
    }

    @Path("{scalableTargetDimensionId}")
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    public ScalingPayload getInstances(@PathParam("scalableTargetDimensionId") String jobId) {
        return Responses.fromSingleValueObservable(awsGatewayCallbackService.getJobInstances(jobId));
    }

    @Path("{scalableTargetDimensionId}")
    @PATCH
    @Produces({MediaType.APPLICATION_JSON})
    @Consumes({MediaType.APPLICATION_JSON})
    public ScalingPayload setInstances(@PathParam("scalableTargetDimensionId") String jobId, ScalingPayload scalingPayload) {
        return Responses.fromSingleValueObservable(awsGatewayCallbackService.setJobInstances(jobId, scalingPayload));
    }
}
