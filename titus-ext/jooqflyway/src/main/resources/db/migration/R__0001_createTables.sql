CREATE SCHEMA IF NOT EXISTS jobactivity;


CREATE TABLE IF NOT EXISTS jobactivity.jobs
(
  job_id          VARCHAR (64) NOT NULL,
  create_time     TIMESTAMP NOT NULL,
  record_time     TIMESTAMP NOT NULL,
  job_record_json jsonb,
  PRIMARY KEY (job_id)
);


CREATE TABLE IF NOT EXISTS jobactivity.tasks
(
    task_id          VARCHAR (64) NOT NULL,
    create_time      TIMESTAMP NOT NULL,
    record_time      TIMESTAMP NOT NULL,
    task_record_json JSONB,
    PRIMARY KEY (task_id)
);