set serveroutput on;
clear screen;
declare
  db_link all_db_links.db_link%type := 'RCAWINBIFRTEST_EDWTS';
  logfile varchar2(128 char) := 'import.log';
  logfile_dir all_directories.directory_name%type := 'DATA_PUMP_DIR';

  dp_h number;
  job_state user_datapump_jobs.state%type;
  job_status ku$_status1010;
  
  procedure fmt_println(text in varchar2, value in varchar2 default null) as
    v_rpad constant number := 30;
  begin
    dbms_output.put_line (rpad(text||' ', v_rpad, '.')||' '||nvl(value, '(NULL)'));
  end fmt_println;

  procedure print_dp_errors (h in number) as
    job_state user_datapump_jobs.state%type;
    job_status ku$_status1010;
  begin
    dbms_datapump.get_status (
      handle => h,
      mask => dbms_datapump.ku$_status_job_error,
      job_state => job_state,
      status => job_status
    );
  
    if job_status.error is not null then
      for i in 1 .. job_status.error.count
      loop
        dbms_output.put_line (
          'Line '||job_status.error(i).loglinenumber||': '||
          'ORA'||job_status.error(i).errornumber||': '||
          job_status.error(i).logtext
        );
      end loop;
    end if;
  end print_dp_errors;

begin

-- Open the job
  dp_h := dbms_datapump.open (
    operation => 'IMPORT',
    job_mode => 'FULL',
    remote_link => db_link
  );

-- Add a logfile
  dbms_datapump.add_file (
     handle => dp_h,
     filename => logfile,
     directory => logfile_dir,
     filetype => dbms_datapump.ku$_file_type_log_file
   );

-- Data filter (Metadata only)
  dbms_datapump.data_filter (
    handle => dp_h,
    name => 'INCLUDE_ROWS',
    value => 0
  );

-- Print job description
  dbms_datapump.get_status (
    handle => dp_h,
    mask => dbms_datapump.ku$_status_job_desc,
    job_state => job_state,
    status => job_status
  );

  fmt_println('JOB_NAME', job_status.job_description.job_name);
  fmt_println('GUID', job_status.job_description.guid);
  fmt_println('OPERATION', job_status.job_description.operation);
  fmt_println('JOB_MODE', job_status.job_description.job_mode);
  fmt_println('REMOTE_LINK', job_status.job_description.remote_link);
  fmt_println('OWNER', job_status.job_description.owner);
  fmt_println('INSTANCE', job_status.job_description.instance);
  fmt_println('DB_VERSION', job_status.job_description.db_version);
  fmt_println('CREATOR_PRIVS', job_status.job_description.creator_privs);
  fmt_println('START_TIME', job_status.job_description.start_time);
  fmt_println('MAX_DEGREE', job_status.job_description.max_degree);
  fmt_println('LOG_FILE', job_status.job_description.log_file);
  fmt_println('SQL_FILE', job_status.job_description.sql_file);

  if job_status.job_description.params is not null then
    for i in 1 .. job_status.job_description.params.count
    loop
      fmt_println (
        text => 'PARAM('||i||')',
        value =>
          job_status.job_description.params(i).param_name||' = '||
          nvl(job_status.job_description.params(i).param_value_n, '"'||job_status.job_description.params(i).param_value_t||'"')
      );
    end loop;
  end if;

-- Run job
  dbms_datapump.start_job (
    handle => dp_h
  );

-- Print any errors
    print_dp_errors (
      h => dp_h
    );

exception
  when others then
    dbms_output.put_line(sqlerrm);

    print_dp_errors (
      h => dp_h
    );

    dbms_datapump.stop_job (
      handle => dp_h
    );

end;
/