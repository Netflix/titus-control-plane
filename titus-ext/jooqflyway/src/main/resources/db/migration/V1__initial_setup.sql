CREATE SCHEMA IF NOT EXISTS jobactivity;


CREATE TABLE IF NOT EXISTS jobactivity.jobs
(
    job_id          VARCHAR (64) NOT NULL,
    create_time     TIMESTAMP NOT NULL,
    record_time     TIMESTAMP NOT NULL,
    job_record_json varchar(64),
    PRIMARY KEY (job_id)
);


CREATE TABLE IF NOT EXISTS jobactivity.tasks
(
    task_id          VARCHAR (64) NOT NULL,
    create_time      TIMESTAMP NOT NULL,
    record_time      TIMESTAMP NOT NULL,
    task_record_json varchar(64),
    PRIMARY KEY (task_id)
);