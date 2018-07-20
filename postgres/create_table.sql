CREATE TABLE tasks (
    modified timestamp NOT NULL DEFAULT NOW(),
    task_id varchar(22) NOT NULL,
    run_id int NOT NULL,
    state text NOT NULL,
    exception_reason text,
    created timestamp NOT NULL,
    scheduled timestamp,
    started timestamp,
    resolved timestamp,
    duration int,
    source text,
    owner text,
    project text,
    revision text,
    push_id int,
    scheduler text,
    provisioner text,
    worker_id text,
    worker_type text,
    worker_group text,
    platform text,
    job_kind text,
    CONSTRAINT dup_task_run UNIQUE (task_id, run_id)
);

CREATE INDEX tasks_worker_id_group_idx ON tasks (worker_id, worker_group) WHERE worker_id IS NOT null AND worker_group IS NOT null;
CREATE INDEX tasks_only_worker_id_idx ON tasks (worker_id) WHERE worker_id IS NOT null;
CREATE INDEX project_idx ON tasks (project DESC NULLS LAST);
CREATE INDEX revision_idx ON tasks (revision DESC NULLS LAST);
CREATE INDEX created_year_idx ON tasks (EXTRACT(YEAR FROM created));
CREATE INDEX created_month_idx ON tasks (EXTRACT(MONTH FROM created));

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;	
END;
$$ language 'plpgsql';

CREATE TRIGGER update_modtime BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE PROCEDURE  update_modified_column();

create TABLE cost_per_workertype (
    workertype text,
    cost money,
    CONSTRAINT unique_workertype UNIQUE (workertype)
);

create TABLE cached_task_definitions (
    timestamp timestamp NOT NULL DEFAULT NOW(),
    task_id varchar(22) NOT NULL,
    definition JSON NOT NULL
);

create index cached_task_id_for_definition on cached_task_definitions (task_id);

CREATE FUNCTION expire_old_task_definitions() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM cached_task_definitions WHERE timestamp < NOW() - INTERVAL '3 hours';
  RETURN NEW;
END;
$$;

CREATE TRIGGER expire_delete_task_definitions_trigger
    AFTER INSERT ON cached_task_definitions
    EXECUTE PROCEDURE expire_old_task_definitions();

CREATE TABLE worker_type_monthly_costs (
    modified timestamp NOT NULL DEFAULT NOW(),
    year int NOT NULL,
    month int NOT NULL,
    provider varchar(10),
    provisioner varchar(32),
    worker_type varchar(60),
    usage_hours numeric(20,2),
    cost numeric(20,2),
    CONSTRAINT dup_worker_type UNIQUE (year, month, provider, provisioner, worker_type)
);
