CREATE TABLE tasks
(
  task_id bigserial NOT NULL,
  worker text NOT NULL,
  insert_time timestamp without time zone NOT NULL DEFAULT now(),
  begin_time timestamp without time zone,
  end_time timestamp without time zone,
  status task_status NOT NULL DEFAULT 'queued'::task_status,
  parameters hstore NOT NULL DEFAULT ''::hstore,
  data bytea,
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

CREATE OR REPLACE FUNCTION tasks_notify_trigger()
  RETURNS trigger AS
$BODY$
begin
  IF NEW.status = 'queued' THEN
	NOTIFY "pgturtle";
  END IF;
  RETURN NULL;
end
$BODY$
  LANGUAGE plpgsql VOLATILE;

CREATE TRIGGER tasks_inserted_trigger
  AFTER INSERT OR UPDATE
  ON tasks
  FOR EACH ROW
  EXECUTE PROCEDURE tasks_notify_trigger();
