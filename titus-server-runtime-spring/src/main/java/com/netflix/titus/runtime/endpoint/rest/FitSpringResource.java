/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import com.netflix.titus.common.framework.fit.FitAction;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitUtil;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.Fit;
import com.netflix.titus.runtime.endpoint.fit.ProtobufFitConverters;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/diagnostic/fit")
public class FitSpringResource {

    private final FitFramework fitFramework;

    @Inject
    public FitSpringResource(TitusRuntime titusRuntime) {
        this.fitFramework = titusRuntime.getFitFramework();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/components", produces = "application/json")
    public Fit.FitComponent getFitComponents() {
        return ProtobufFitConverters.toGrpcFitComponent(fitFramework.getRootComponent());
    }

    @RequestMapping(method = RequestMethod.GET, path = "/actionDescriptors", produces = "application/json")
    public Fit.FitActionDescriptors getFitActionDescriptors() {
        List<Fit.FitActionDescriptor> descriptors = fitFramework.getFitRegistry().getFitActionDescriptors().stream()
                .map(ProtobufFitConverters::toGrpcFitActionDescriptor)
                .collect(Collectors.toList());
        return Fit.FitActionDescriptors.newBuilder().addAllDescriptors(descriptors).build();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/actions", produces = "application/json")
    public Fit.FitActions getActions() {
        List<Fit.FitAction> actions = findAllActions(fitFramework.getRootComponent()).stream().map(ProtobufFitConverters::toGrpcFitAction).collect(Collectors.toList());
        return Fit.FitActions.newBuilder().addAllActions(actions).build();
    }

    @RequestMapping(method = RequestMethod.POST, path = "/actions", consumes = "application/json")
    public Response addAction(@RequestBody Fit.AddAction request) {
        FitComponent fitComponent = FitUtil.getFitComponentOrFail(fitFramework, request.getComponentId());
        FitInjection fitInjection = FitUtil.getFitInjectionOrFail(request.getInjectionId(), fitComponent);

        Function<FitInjection, FitAction> fitActionFactory = fitFramework.getFitRegistry().newFitActionFactory(
                request.getActionKind(), request.getActionId(), request.getPropertiesMap()
        );
        fitInjection.addAction(fitActionFactory.apply(fitInjection));

        return Response.noContent().build();
    }

    @RequestMapping(method = RequestMethod.DELETE, path = "/actions/{actionId}")
    public Response deleteAction(@PathVariable("actionId") String actionId,
                                 @RequestParam("componentId") String componentId,
                                 @RequestParam("injectionId") String injectionId) {
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
