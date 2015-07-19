CREATE TABLE tasks
(
  task_id bigserial NOT NULL,
  worker text NOT NULL,
  insert_time timestamp without time zone NOT NULL DEFAULT now(),
  begin_time timestamp without time zone,
  end_time timestamp without time zone,
  status task_status NOT NULL DEFAULT 'queued'::task_status,
  parameters bytea NOT NULL DEFAULT ''::bytea,
  result bytea,
  CONSTRAINT tasks_pkey PRIMARY KEY (task_id)
);

CREATE TYPE task_status AS ENUM
(
  'waiting',
  'queued',
  'running',
  'completed',
  'failed'
);
