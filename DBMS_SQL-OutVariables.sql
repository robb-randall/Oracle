set serveroutput on;
clear screen;
declare
  stmt varchar2(32767 char) := 'BEGIN DBMS_LOCK.SLEEP(2); END;';
  db_link user_db_links.db_link%type := 'RCAWINBIFRTEST_EDWDV';
  v_start_ts timestamp;
  v_end_ts timestamp;
  v_sqlrowcount number;
  v_sqlcode number;
  v_sqlerrm varchar2 (4000 char);

  v_rpad number := 30;

  sql_h number;
  recs number;
  sql_stmt clob :=
q'{declare sql_h number := dbms_sql.open_cursor<<db_link>> ();
begin
  dbms_sql.parse<<db_link>> (c => sql_h, statement => :statement, language_flag => dbms_sql.native);
  :start_ts := current_timestamp;
  begin
    :sqlrowcount := dbms_sql.execute<<db_link>> (c => sql_h);
  end;
  :sqlcode := sqlcode;
  :sqlerrm := sqlerrm;
  :end_ts := current_timestamp;
  dbms_sql.close_cursor<<db_link>> (c => sql_h);
end;}';

begin

--
-- Open the cursor
--
  sql_h := dbms_sql.open_cursor();

--
-- Parse the statement
--
  dbms_sql.parse (
    c => sql_h,
    statement => replace (sql_stmt, '<<db_link>>', rtrim('@'||db_link, '@')),
    language_flag => dbms_sql.native
  );

--
-- Bind variables
--
  dbms_sql.bind_variable (
    c => sql_h,
    name => ':statement',
    value => stmt
  );

  dbms_sql.bind_variable (
    c => sql_h,
    name => ':start_ts',
    value => v_start_ts
  );

  dbms_sql.bind_variable (
    c => sql_h,
    name => ':sqlrowcount',
    value => v_sqlrowcount
  );

  dbms_sql.bind_variable (
    c => sql_h,
    name => ':end_ts',
    value => v_end_ts
  );

  dbms_sql.bind_variable (
    c => sql_h,
    name => ':sqlcode',
    value => v_sqlcode
  );

  dbms_sql.bind_variable (
    c => sql_h,
    name => ':sqlerrm',
    value => v_sqlerrm,
    out_value_size => 4000
  );

--
-- Execute the statement
--
  recs := dbms_sql.execute (
    c => sql_h
  );

--
-- Get OUT variables value
--
  dbms_sql.variable_value (
    c => sql_h,
    name => ':start_ts',
    value => v_start_ts
  );

  dbms_sql.variable_value (
    c => sql_h,
    name => ':sqlrowcount',
    value => v_sqlrowcount
  );

  dbms_sql.variable_value (
    c => sql_h,
    name => ':end_ts',
    value => v_end_ts
  );

  dbms_sql.variable_value (
    c => sql_h,
    name => ':sqlcode',
    value => v_sqlcode
  );

  dbms_sql.variable_value (
    c => sql_h,
    name => ':sqlerrm',
    value => v_sqlerrm
  );

--
-- Print outcome
--
  dbms_output.put_line (
    '+ Statement ran'||chr(13)||
    rpad('|- Statement ', v_rpad, '.')||' '||substr(stmt, 1, 100)||chr(13)||
    rpad('|- DB Link ', v_rpad, '.')||' '||db_link||chr(13)||
    rpad('|- Start TS ', v_rpad, '.')||' '||v_start_ts||chr(13)||
    rpad('|- End TS ', v_rpad, '.')||' '||v_end_ts||chr(13)||
    rpad('|- Elapsed ', v_rpad, '.')||' '||to_char(v_end_ts-v_start_ts)||chr(13)||
    rpad('|- SQL Rowcount ', v_rpad, '.')||' '||v_sqlrowcount||chr(13)||
    rpad('|- SQL Code ', v_rpad, '.')||' '||v_sqlcode||chr(13)||
    rpad('|- SQL Error ', v_rpad, '.')||' '||v_sqlerrm
  );

--
-- Close cursor
--
  dbms_sql.close_cursor (
    c => sql_h
  );

end;
/