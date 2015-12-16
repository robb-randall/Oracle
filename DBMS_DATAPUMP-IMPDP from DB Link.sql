set serveroutput on;
set feedback off;
clear screen;

/* Needed permissions:
GRANT READ,WRITE ON DIRECTORY DATA_PUMP_DIR;
GRANT IMP_FULL_DATABASE;
*/

declare
  job_exists exception;
  pragma exception_init (job_exists, -31634);

  v_handle number;
  v_job_name varchar2 (30) := 'IMP$FILE_LOAD_TABLES';
  v_job_state varchar2 (128);
begin

-- Open the job
  dbms_output.put_line ('Opening job');
  v_handle := dbms_datapump.open
  (
    operation => 'IMPORT',
    job_mode => 'TABLE',
    remote_link => 'PROD',
    job_name => v_job_name,
    version => 'COMPATIBLE',
    compression => dbms_datapump.ku$_compress_metadata
  );

-- Add log file
  dbms_output.put_line ('Adding log file');
  dbms_datapump.add_file
  (
    handle => v_handle,
    filename => v_job_name||'.log',
    directory => 'DATA_PUMP_DIR',
    filetype => dbms_datapump.ku$_file_type_log_file
  );

-- Set parameters
  dbms_output.put_line ('Setting Paramters');
  dbms_datapump.set_parameter
  (
    handle => v_handle,
    name => 'TABLE_EXISTS_ACTION',
    value => 'REPLACE'
  );

-- Set metadata filters
  dbms_output.put_line ('Setting metadata filter (Schema)');
  dbms_datapump.metadata_filter
  (
    handle => v_handle,
    name => 'SCHEMA_LIST',
    value => q'{'BI_PROD'}'
  );

  dbms_output.put_line ('Setting metadata filter (Table)');
  dbms_datapump.metadata_filter
  (
    handle => v_handle,
    name => 'NAME_EXPR',
    value => q'{IN ('STG_OFFSITE_PROJECTIONS','STG_DM_APPEALS','STG_DM_FULFILLMENT','STG_DM_EDUCATION','STG_DM_AUDIENCE')}',
    object_path => 'TABLE'
  );

-- Transform schema
--  dbms_output.put_line ('Remaping schema');
--  dbms_datapump.metadata_remap
--  (
--    handle => v_handle,
--    name => 'REMAP_SCHEMA',
--    old_value => 'BI_PROD',
--    value => user,
--    object_type => NULL
--  );

-- Run the job
  dbms_output.put_line ('Running Job');
  dbms_datapump.start_job
  (
    handle => v_handle,
    skip_current => 0
  );

-- Wait for job
  dbms_output.put_line ('Waiting for job');
  dbms_datapump.wait_for_job
  (
    handle => v_handle,
    job_state => v_job_state
  );

  dbms_output.put_line ('Import completed with a status of "'||v_job_state||'"');

exception

  when job_exists then
    dbms_output.put_line ('Job exists, stopping job then try to re-run.');

    dbms_datapump.stop_job
    (
      handle => dbms_datapump.attach (job_name => v_job_name, job_owner => user),
      immediate => 1,
      keep_master => 0,
      delay => 0
    );

    raise;

  when others then
    dbms_output.put_line (sqlerrm);

    dbms_output.put_line ('Stopping Job');
    dbms_datapump.stop_job
    (
      handle => v_handle,
      immediate => 1,
      keep_master => 0,
      delay => 0
    );

    raise;

end;
/