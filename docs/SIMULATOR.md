# How to run cloud simulator

To start cloud simulator run `io.netflix.titus.testkit.embedded.cloud.SimulatedCloudRunner` class. The simulator is
populated with three instance group on startup:
* critical1 (instance type m4.4xlarge)
* flex1 (instance type r4.8xlarge)
* flexGpu (instance type g2.8xlarge)

The simulator runs two server endpoints:
* REST/HTTP on port 8099 
* GRPC on port 7006

Cloud simulator REST API:
|Method   |Resource|Description|
|---------|--------|-----------|
|GET      | [http://localhost:8099/cloud/agents/instanceGroups](http://localhost:8099/cloud/agents/instanceGroups) | Returns agent instance groups. |
|GET      | [http://localhost:8099/cloud/agents/instances](http://localhost:8099/cloud/agents/instances) | Returns agent instances. |
|GET      | [http://localhost:8099/cloud/agents/instanceGroups/{instanceGroupId}/instances](http://localhost:8099/cloud/agents/instanceGroups/{instanceGroupId}/instances) | Returns agent instances of an instance group. |
|GET      | [http://localhost:8099/cloud/agents/tasks](http://localhost:8099/cloud/agents/tasks) | Returns all tasks running in the simulated cloud. |
|GET      | [http://localhost:8099/cloud/agents/instances/{instanceId}/tasks](http://localhost:8099/cloud/agents/instances/{instanceId}/tasks) | Returns all tasks running on an agent instance. |
|PUT      | [http://localhost:8099/cloud/agents/instanceGroups/{instanceGroupId}/capacity](http://localhost:8099/cloud/agents/instanceGroups/{instanceGroupId}/capacity) | Changes instance group capacity. | 
|DELETE   | [http://localhost:8099/cloud/agents/instances/{instanceId}](http://localhost:8099/cloud/agents/instances/{instanceId}) | An agent instance terminate and shrink. | 

# How to run embedded Titus control plane stack

To start embedded Titus stack that connects to simulated cloud instance run `io.netflix.titus.testkit.embedded.stack.EmbeddedTitusStackRunner` 
with hostname and port number of cloud simulator GRPC endpoint.