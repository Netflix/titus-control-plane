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

# Scripting container transition rules

To simulate container behavior (state transition delay, failures), a script can be embedded into a job descriptor,
which is read by the cloud simulator and executed.

Example 1:  
`TASK_LIFECYCLE_1=selector: slots=0.. slotStep=2; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s}`  
`TASK_LIFECYCLE_2=selector: slots=1.. slotStep=2; launched: delay=2s; startInitiated: finish=failed}`  

Example 2:  
`TASK_LIFECYCLE_1=selector: resubmits=0,1 slots=0.. slotStep=2; launched: delay=2s; startInitiated: finish=failed}`  
`TASK_LIFECYCLE_2=selector: resubmits=2..; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s}`  


`selector` defines subset of tasks for which a particular rule is applied:
* `slots` refers to a task index, which in case of a batch
job is part of the task definition, and in case of service job is assigned by the cloud simulator in the order of task arrival.
* `slotStep` step applied to task index range defined by `slots` parameter
* `resubmits` refers to a resubmit number(s) of a task at a given index
* `slotStep` step applied to task resubmit range defined by `resubmits` parameter

The rules may be defined for states: `launched`, `startInitiated`, `started` and `killInitiated`. The following parameters
are possible in each state:
* `delay` which defines how long the container should stay in this state, before moving to the next one
* `finish` which instructs to fail the container in the given state, with the provided status (possible values are: `failed`, `error`)
