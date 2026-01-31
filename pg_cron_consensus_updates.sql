-- pg_cron stress updates for consensus test tables
--
-- Prerequisites:
--   - pg_cron must be in shared_preload_libraries
--     (e.g. docker run ... -c shared_preload_libraries=pg_cron)
--   - Tables foo_1..foo_5000 must exist in the 'test' database
--     (created by setup_consensus_test.sh)
--
-- Each second, one quarter of the tables (1250) get an UPDATE.
-- Over 4 seconds every table is touched once.

-- pg_cron extension lives in the postgres database
--\c test
--CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Create the update function in the test database
\c test

CREATE OR REPLACE FUNCTION update_consensus_batch() RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    batch_num int := extract(epoch FROM now())::bigint % 4;  -- 0-3
    start_idx int := batch_num * 1250 + 1;
    end_idx   int := (batch_num + 1) * 1250;
    row_id    int := 1 + (extract(epoch FROM now())::bigint % 1000);
    i int;
BEGIN
    FOR i IN start_idx..end_idx LOOP
        EXECUTE format(
            'UPDATE foo_%s SET data = md5(now()::text), ts = now() WHERE id = %s',
            i, row_id
        );
    END LOOP;
END;
$$;

-- Schedule the job (runs every 1 second, requires pg_cron >= 1.5)
--\c test
--SELECT cron.schedule('update_consensus', '1 second', 'SELECT update_consensus_batch()');
--UPDATE cron.job SET database = 'test' WHERE jobname = 'update_consensus';
--
-- Verify
--SELECT * FROM cron.job WHERE jobname = 'update_consensus';
