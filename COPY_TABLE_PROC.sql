create or replace procedure copy_table
(
  src_owner in all_tables.owner%type,
  src_table in all_tables.table_name%type,
  src_db_link in user_db_links.db_link%type,
  dst_owner in all_tables.owner%type default user,
  dst_table in all_tables.table_name%type default null,
  remap_tablespace in varchar2, -- 'OLD_TBLSP:NEW_TBLSP'
  log_dir in all_directories.directory_name%type default 'DATA_PUMP_DIR',
  table_exists_action in varchar2 default 'SKIP', -- TRUNCATE, REPLACE, APPEND, and SKIP
  wait_to_finish in boolean default true,
  spool_log_file in boolean default false,
  delete_log_file in boolean default true
) as
/*******************************************************************************
Needed Grants:
 - READ and WRITE on LOG_DIR
 - DATAPUMP_IMP_FULL_DATABASE on local database
 - DATAPUMP_EXP_FULL_DATABASE on remote database
*******************************************************************************/
  job_exists exception;
  pragma exception_init (job_exists, -31634);

  v_handle number;
  v_job_name constant user_datapump_jobs.job_name%type := 'IMP$'||nvl (dst_table, src_table);
  v_log_file constant varchar2 (128) := v_job_name||'.log';
  v_job_state user_datapump_jobs.state%type;
  v_src_tablespace user_tablespaces.tablespace_name%type;
  v_dst_tablespace user_tablespaces.tablespace_name%type;
  v_file_handle utl_file.file_type;
  v_file_buffer varchar2 (32767);
begin

-- Open the job
  dbms_output.put_line ('Opening datapump job '||v_job_name);
  v_handle := dbms_datapump.open
  (
    operation => 'IMPORT',
    job_mode => 'TABLE',
    remote_link => src_db_link,
    job_name => v_job_name,
    version => 'COMPATIBLE',
    compression => dbms_datapump.ku$_compress_metadata
  );

-- Add logfile
  dbms_output.put_line ('Adding log file '||log_dir||':'||v_log_file);
  dbms_datapump.add_file
  (
    handle => v_handle,
    filename => v_log_file,
    directory => log_dir,
    filetype => dbms_datapump.ku$_file_type_log_file
  );

-- Setting table exists action
  dbms_output.put_line ('Setting TABLE_EXISTS_ACTION='||table_exists_action);
  dbms_datapump.set_parameter
  (
    handle => v_handle,
    name => 'TABLE_EXISTS_ACTION',
    value => table_exists_action
  );

-- Set schema metadata filter
  dbms_output.put_line ('Setting SCHEMAS='||src_owner);
  dbms_datapump.metadata_filter
  (
    handle => v_handle,
    name => 'SCHEMA_LIST',
    value => q'{'}'||src_owner||q'{'}'
  );

-- Set table metadata filter
  dbms_output.put_line ('Setting TABLES='||src_table);
  dbms_datapump.metadata_filter
  (
    handle => v_handle,
    name => 'NAME_EXPR',
    value => q'{IN ('}'||src_table||q'{')}',
    object_path => 'TABLE'
  );

-- Remap tablespace
  if remap_tablespace is not null then
    v_src_tablespace := regexp_substr (remap_tablespace, '[^:]+', 1, 1);
    v_dst_tablespace := regexp_substr (remap_tablespace, '[^:]+', 1, 2);
    dbms_output.put_line ('REMAP_TABLESPACE='||remap_tablespace);
    dbms_datapump.metadata_remap
    (
      handle => v_handle,
      name => 'REMAP_TABLESPACE',
      old_value => v_src_tablespace,
      value => v_dst_tablespace,
      object_type => NULL
    );
  end if;

-- Remap schema
  if src_owner != dst_owner then
    dbms_output.put_line ('REMAP_SCHEMA='||src_owner||':'||dst_owner);
    dbms_datapump.metadata_remap
    (
      handle => v_handle,
      name => 'REMAP_SCHEMA',
      old_value => src_owner,
      value => dst_owner,
      object_type => NULL
    );
  end if;

-- Remap table
  if src_table != dst_table then
    dbms_output.put_line ('REMAP_TABLE='||src_table||':'||dst_table);
    dbms_datapump.metadata_remap
    (
      handle => v_handle,
      name => 'REMAP_TABLE',
      old_value => src_table,
      value => dst_table,
      object_type => NULL
    );
  end if;

-- Run the job
  dbms_output.put_line ('Running Job');
  dbms_datapump.start_job
  (
    handle => v_handle,
    skip_current => 0
  );

-- Wait for job to finish
  if wait_to_finish then
    dbms_datapump.wait_for_job
    (
      handle => v_handle,
      job_state => v_job_state
    );
    dbms_output.put_line ('Job finished with a status of '||v_job_state);
  else
    dbms_output.put_line ('Job started');
  end if;

-- Spool log file
  if spool_log_file then
    dbms_output.put_line (chr(13)||'## START LOGFILE='||log_dir||':'||v_log_file);

    v_file_handle := utl_file.fopen
    (
      location => log_dir,
      filename => v_log_file,
      open_mode => 'R',
      max_linesize => 32767
    );

    begin
      loop
        utl_file.get_line
        (
          file => v_file_handle,
          buffer => v_file_buffer,
          len => 32767
        );
        dbms_output.put_line (v_file_buffer);
      end loop;
    exception
      when no_data_found then
        null; -- End of file
    end;

    utl_file.fclose (file => v_file_handle);

    dbms_output.put_line ('## END LOGFILE='||log_dir||':'||v_log_file);
  end if;

-- Delete log file
  if delete_log_file then
    dbms_output.put_line ('Deleting log file '||log_dir||':'||v_log_file);
    utl_file.fremove
    (
      location => log_dir,
      filename => v_log_file
    );
  end if;

exception

  when job_exists then
    raise_application_error (-20000, 'Job exists, drop the job with the following command and try again:'||chr(13)||
      q'{exec dbms_datapump.stop_job (dbms_datapump.attach ('}'||v_job_name||q'{', user), 1, 0, 0);}');

  when others then

    dbms_output.put_line ('Stopping Job');
    dbms_datapump.stop_job
    (
      handle => v_handle,
      immediate => 1,
      keep_master => 0,
      delay => 0
    );

    raise;

end copy_table;
/
