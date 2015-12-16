set serveroutput on;
clear screen;

declare
  table_name varchar2(65 char) := '"BI_PROD"."ACTIVE_DATAMART"';
  logfile varchar2(128 char) := 'DATA_PUMP_DIR:'||schema_name||'_'||to_char(sysdate, 'RRRR-MM-DD_HH24-MI')||'.log';
  dmpfile varchar2(128 char) := 'DATA_PUMP_DIR:'||schema_name||'_'||to_char(sysdate, 'RRRR-MM-DD_HH24-MI')||'.dmp';

  dp_h number;
  job_state user_datapump_jobs.state%type;
  job_status sys.ku$_status1010;

  v_rpad constant number := 30;

  v_table varchar2 (32 char);
  v_owner varchar2 (32 char);
begin

  -- Assign owner and table
  v_table := regexp_substr(table_name, '[^\.]+', 1, 2);

  if v_table is null then
    v_table := regexp_substr(table_name, '[^\.]+', 1, 1);
    v_owner := user;
  else
    v_owner := regexp_substr(table_name, '[^\.]+', 1, 1);
  end if;
    

-- Open
  dp_h := dbms_datapump.open (
    operation => 'EXPORT',
    job_mode => 'SCHEMA',
    remote_link => null,
    job_name => null,
    version => 'COMPATIBLE'
  );

-- Add files
  dbms_datapump.add_file (
    handle => dp_h,
    filename => regexp_substr (logfile, '[^:]+', 1, 2),
    directory => regexp_substr (logfile, '[^:]+', 1, 1),
    filetype => dbms_datapump.ku$_file_type_log_file,
    reusefile => 1
  );

  dbms_datapump.add_file (
    handle => dp_h,
    filename => regexp_substr (dmpfile, '[^:]+', 1, 2),
    directory => regexp_substr (dmpfile, '[^:]+', 1, 1),
    filetype => dbms_datapump.ku$_file_type_dump_file,
    reusefile => 1
  );

-- Set schema filter
  dbms_datapump.metadata_filter (
    handle => dp_h,
    name => 'SCHEMA_EXPR',
    value => q'{='}'||v_owner||q'{'}'
  );

-- Set table filter
  dbms_datapump.metadata_filter (
    handle => dp_h,
    name => 'NAME_EXPR',
    value => q'{='}'||v_owner||q'{'}',
    object_path => 'TABLE_NAME'
  );

-- Start
  dbms_datapump.start_job (
    handle => dp_h
  );

  job_state := 'EXECUTING';

-- Wait and get details
  while job_state = 'EXECUTING'
  loop
    dbms_datapump.get_status (
      handle => dp_h,
      mask =>
        dbms_datapump.ku$_status_job_desc +
        dbms_datapump.ku$_status_job_status +
        dbms_datapump.ku$_status_job_error,
      job_state => job_state,
      status => job_status
    );
  end loop;

-- Print everything
  dbms_output.put_line (
    rpad('JOB_NAME ', v_rpad, '.')||' '||job_status.job_description.job_name||chr(13)||
    rpad('GUID ', v_rpad, '.')||' '||job_status.job_description.guid||chr(13)||
    rpad('OPERATION ', v_rpad, '.')||' '||job_status.job_description.operation||chr(13)||
    rpad('JOB_MODE ', v_rpad, '.')||' '||job_status.job_description.job_mode||chr(13)||
    rpad('REMOTE_LINK ', v_rpad, '.')||' '||nvl(job_status.job_description.remote_link, 'NA')||chr(13)||
    rpad('OWNER ', v_rpad, '.')||' '||job_status.job_description.owner||chr(13)||

    rpad('SCHEMA ', v_rpad, '.')||' '||schema_name||chr(13)||
    rpad('LOGFILE ', v_rpad, '.')||' '||logfile||chr(13)||
    rpad('DMPFILE ', v_rpad, '.')||' '||dmpfile||chr(13)||

    rpad('INSTANCE ', v_rpad, '.')||' '||job_status.job_description.instance||chr(13)||
    rpad('DB_VERSION ', v_rpad, '.')||' '||job_status.job_description.db_version||chr(13)||
    rpad('CREATOR_PRIVS ', v_rpad, '.')||' '||job_status.job_description.creator_privs||chr(13)||
    rpad('START_TIME ', v_rpad, '.')||' '||job_status.job_description.start_time||chr(13)||
    rpad('MAX_DEGREE ', v_rpad, '.')||' '||job_status.job_description.max_degree||chr(13)||
    rpad('LOG_FILE ', v_rpad, '.')||' '||job_status.job_description.log_file||chr(13)||
    rpad('SQL_FILE ', v_rpad, '.')||' '||nvl(job_status.job_description.sql_file, 'NA')||chr(13)||

    rpad('BYTES_PROCESSED ', v_rpad, '.')||' '||job_status.job_status.bytes_processed||chr(13)||
    rpad('TOTAL_BYTES ', v_rpad, '.')||' '||job_status.job_status.total_bytes||chr(13)||
    rpad('PERCENT_DONE ', v_rpad, '.')||' '||job_status.job_status.percent_done||chr(13)||
    rpad('DEGREE ', v_rpad, '.')||' '||job_status.job_status.degree||chr(13)||
    rpad('ERROR_COUNT ', v_rpad, '.')||' '||job_status.job_status.error_count||chr(13)||
    rpad('PHASE ', v_rpad, '.')||' '||job_status.job_status.phase||chr(13)||
    rpad('RESTART_COUNT ', v_rpad, '.')||' '||job_status.job_status.restart_count
  );

  -- Print parameters
  if job_status.job_description.params is not null then
    for paramno in 1 .. job_status.job_description.params.count
    loop
      dbms_output.put_line (
        rpad('PARAM('||paramno||')', v_rpad, '.')||' '||
        job_status.job_description.params(paramno).param_name||' => "'||
        nvl(to_char(job_status.job_description.params(paramno).param_value_n), job_status.job_description.params(paramno).param_value_t)||'"'
      );
    end loop;
  end if;

  -- Print files
  if job_status.job_status.files is not null then
    for fileno in 1 .. job_status.job_status.files.count
    loop
      dbms_output.put_line(rpad('FILE('||fileno||') ', v_rpad, '.')||' '||job_status.job_status.files(fileno).file_name);
    end loop;
  end if;

  -- Print any errors
  if job_status.error is not null then
    dbms_output.put_line('ERRORS ENCOUNTERED:');
    for errno in 1 .. job_status.error.count
    loop
      dbms_output.put_line(rpad('LINE('||job_status.error(errno).loglinenumber||')', v_rpad, '.')||' '||job_status.error(errno).logtext);
    end loop;
  end if;

exception
  when others then

    -- Print useless error
    dbms_output.put_line(sqlerrm);

    -- Print useful errors
    dbms_datapump.get_status (
      handle => dp_h,
      mask => dbms_datapump.ku$_status_job_error,
      job_state => job_state,
      status => job_status
    );

    for errno in 1 .. job_status.error.count
    loop
      dbms_output.put_line(rpad('LINE '||job_status.error(errno).loglinenumber, v_rpad, '.')||' '||job_status.error(errno).logtext);
    end loop;

    -- Stop the job
    dbms_datapump.stop_job (
      handle => dp_h
    );

end;
/


