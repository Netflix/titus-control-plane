package io.netflix.titus.runtime.endpoint.fit;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.netflix.titus.runtime.Fit;
import io.netflix.titus.common.framework.fit.FitAction;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitFramework;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.framework.fit.FitUtil;
import io.netflix.titus.common.runtime.TitusRuntime;

@Path("/api/diagnostic/fit")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class FitResource {

    private final FitFramework fitFramework;

    @Inject
    public FitResource(TitusRuntime titusRuntime) {
        this.fitFramework = titusRuntime.getFitFramework();
    }

    @GET
    @Path("/components")
    public Fit.FitComponent getFitComponents() {
        return ProtobufFitConverters.toGrpcFitComponent(fitFramework.getRootComponent());
    }

    @GET
    @Path("/actionDescriptors")
    public List<Fit.FitActionDescriptor> getFitActionDescriptors() {
        return fitFramework.getFitRegistry().getFitActionDescriptors().stream()
                .map(ProtobufFitConverters::toGrpcFitActionDescriptor)
                .collect(Collectors.toList());
    }

    @GET
    @Path("/actions")
    public List<Fit.FitAction> getActions() {
        return findAllActions(fitFramework.getRootComponent()).stream().map(ProtobufFitConverters::toGrpcFitAction).collect(Collectors.toList());
    }

    @POST
    @Path("/actions")
    public Response addAction(Fit.AddAction request) {
        FitComponent fitComponent = FitUtil.getFitComponentOrFail(fitFramework, request.getComponentId());
        FitInjection fitInjection = FitUtil.getFitInjectionOrFail(request.getInjectionId(), fitComponent);

        Function<FitInjection, FitAction> fitActionFactory = fitFramework.getFitRegistry().newFitActionFactory(
                request.getActionKind(), request.getActionId(), request.getPropertiesMap()
        );
        fitInjection.addAction(fitActionFactory.apply(fitInjection));

        return Response.noContent().build();
    }

    @DELETE
    @Path("/actions/{actionId}")
    public Response deleteAction(@PathParam("actionId") String actionId,
                                 @QueryParam("componentId") String componentId,
                                 @QueryParam("injectionId") String injectionId) {
        FitInjection fitInjection = FitUtil.getFitInjectionOrFail(injectionId, FitUtil.getFitComponentOrFail(fitFramework, componentId));
        fitInjection.removeAction(FitUtil.getFitActionOrFail(actionId, fitInjection).getId());
        return Response.noContent().build();
    }

    private List<FitAction> findAllActions(FitComponent fitComponent) {
        List<FitAction> result = new ArrayList<>();
        fitComponent.getInjections().forEach(i -> result.addAll(i.getActions()));
        fitComponent.getChildren().forEach(c -> result.addAll(findAllActions(c)));
        return result;
    }
}
