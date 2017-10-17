// ------------------------------------------------------------------
// Job Management schema

CREATE TABLE "active_job_ids" (
  bucket int,
  job_id text,
  PRIMARY KEY (bucket, job_id)
) WITH
  comment='The active set of job ids' AND
  compression={};

CREATE TABLE "active_task_ids" (
  job_id text,
  task_id text,
  PRIMARY KEY ((job_id), task_id)
) WITH
  comment='The active set of task ids as well as the job id they belong to' AND
  compression={};

CREATE TABLE "active_jobs" (
  job_id text,
  value text,
  PRIMARY KEY (job_id)
) WITH
  comment='The active jobs' AND
  compression={};

CREATE TABLE "active_tasks" (
  task_id text,
  value text,
  PRIMARY KEY (task_id)
) WITH
  comment='The active tasks' AND
  compression={};

CREATE TABLE "archived_jobs" (
  job_id text,
  value text,
  PRIMARY KEY (job_id)
) WITH
  comment='The archived jobs' AND
  compression={};

CREATE TABLE "archived_task_ids" (
  job_id text,
  task_id text,
  PRIMARY KEY ((job_id), task_id)
) WITH
  comment='The archived set of task ids as well as the job id they belong to' AND
  compression={};

CREATE TABLE "archived_tasks" (
  task_id text,
  value text,
  PRIMARY KEY (task_id)
) WITH
  comment='The archived tasks' AND
  compression={};

// ------------------------------------------------------------------
// Agent Management schema

CREATE TABLE "agent_instance_groups" (
  id text,
  value text,
  PRIMARY KEY (id)
) WITH
  comment='The agent instance groups' AND
  compression={};

CREATE TABLE "agent_instances" (
  id text,
  value text,
  PRIMARY KEY (id)
) WITH
  comment='The agent instances' AND
  compression={};

// ------------------------------------------------------------------
// Autoscaling schema
CREATE TABLE "app_scale_policy" (
	ref_id uuid,
	job_id text,
	policy_id text,
	alarm_id text,
	status text,
	status_message text,
	value text,
	PRIMARY KEY (ref_id)
) WITH
  comment='App auto scale policies' AND
  compression={};

CREATE TABLE "app_scale_jobs" (
	job_id text,
	ref_id uuid,
	PRIMARY KEY (job_id, ref_id)
) WITH
  comment='App auto scale jobs' AND
  compression={};