set serveroutput on;
set feedback on;
clear screen;
declare
  in_task_name user_parallel_execute_tasks.task_name%type := '&task_name';

  v_has_errors boolean := false;
begin

  dbms_output.put_line('Running failed chunks for task "'||in_task_name||'"');

  for i in (
    select
      task_name,
      c.chunk_id,
      t.sql_stmt,
      start_rowid,
      end_rowid,
      start_id,
      end_id
    from user_parallel_execute_tasks t
    join user_parallel_execute_chunks c
      using (task_name)
    where task_name=in_task_name
      and t.status='FINISHED_WITH_ERROR'
      and c.status='PROCESSED_WITH_ERROR'
  ) loop

    dbms_output.put_line ('+ Processing Chunk ID: "'||i.chunk_id||'"');

    declare
      c_sql number;
      v_rowcount number;
    begin

      -- Open the cursor
      dbms_output.put_line('|- Opening cursor');
      c_sql := dbms_sql.open_cursor();

      -- Parse the statement
      dbms_output.put_line('|- Parsing statement');
      dbms_sql.parse (
        c => c_sql,
        statement => i.sql_stmt,
        language_flag => dbms_sql.native
      );

      -- Bind the "start_id" and ":end_id" variables
      if i.start_rowid is not null then
        -- By ROWID
        dbms_output.put_line('|- Binding "'||rowidtochar(i.start_rowid)||'" to :start_id');
        dbms_sql.bind_variable (
          c => c_sql,
          name => ':start_id' ,
          value => i.start_rowid
        );

        dbms_output.put_line('|- Binding "'||rowidtochar(i.end_rowid)||'" to :end_id');
        dbms_sql.bind_variable (
          c => c_sql,
          name => ':end_id' ,
          value => i.end_rowid
        );

      elsif i.start_id is not null then
        -- By Number
        dbms_output.put_line('|- Binding '||i.start_id||' to :start_id');
        dbms_sql.bind_variable (
          c => c_sql,
          name => ':start_id' ,
          value => i.start_id
        );

        dbms_output.put_line('|- Binding '||i.end_id||' to :end_id');
        dbms_sql.bind_variable (
          c => c_sql,
          name => ':end_id' ,
          value => i.end_id
        );    
      end if;

      -- Update chunk status to ASSIGNED (sets the START_TIMESTAMP)
      dbms_output.put_line('|- Chunk status = "ASSIGNED"');
      dbms_parallel_execute.set_chunk_status (
        task_name => i.task_name,
        chunk_id => i.chunk_id,
        status => dbms_parallel_execute.assigned,
        err_num => 0,
        err_msg => null
      );

      -- Execute the statement
      dbms_output.put_line('|- Executing statement');
      v_rowcount := dbms_sql.execute (
        c => c_sql
      );

      dbms_output.put_line('|- '||v_rowcount||' records processed.');

      -- Update chunk status to PROCESSEED (sets the END_TIMESTAMP)
      dbms_output.put_line('|- Chunk status = "PROCESSEED"');
      dbms_parallel_execute.set_chunk_status (
        task_name => i.task_name,
        chunk_id => i.chunk_id,
        status => dbms_parallel_execute.processed,
        err_num => 0,
        err_msg => null
      );

      -- Close the cursor
      dbms_output.put_line('|- Closing cursor');
      dbms_sql.close_cursor (
        c => c_sql
      );

    exception
      when others then
        dbms_output.put_line('|- Error encountered in step above: '||sqlerrm);

        -- Update chunk status to PROCESSED_WITH_ERROR (sets the END_TIMESTAMP, and error info)
        dbms_output.put_line('|- Chunk status = "PROCESSED_WITH_ERROR"');
        dbms_parallel_execute.set_chunk_status (
          task_name => i.task_name,
          chunk_id => i.chunk_id,
          status => dbms_parallel_execute.processed_with_error,
          err_num => sqlcode,
          err_msg => sqlerrm
        );

        -- Close the cursor if it's open
        if dbms_sql.is_open(c => c_sql) then
          dbms_output.put_line('|- Closing cursor');
          dbms_sql.close_cursor (
            c => c_sql
          );
        end if;

        v_has_errors := true;
    end;
  end loop;

  -- If a chunk fails we want to raise an error that not all chunks processed successfully
  if v_has_errors then
    raise_application_error(-20000, 'Not all chunks processed successfully, check SERVEROUTPUT and USER_PARALLEL_EXECUTE_CHUNKS table for more details.');
  end if;

end rerun_failed_chunks;
/
