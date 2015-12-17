set serveroutput on;
clear screen;
declare
  data_center_id number := &dcid;
  source_db_link all_db_links.db_link%type := '&db_link';

  delete_stmt clob := q'~DELETE FROM ${OWNER}s.${TABLE}s WHERE DATA_CENTER_ID=:data_center_id~';
  insert_stmt clob := q'{INSERT /*+APPEND*/ INTO ${OWNER}s.${TABLE}s SELECT * FROM ${OWNER}s.${TABLE}s@${DB_LINK}s WHERE DATA_CENTER_ID=:data_center_id}';

  function fmt_stmt (
    stmt in clob,
    owner_name in all_tables.owner%type,
    table_name in all_tables.table_name%type,
    db_link in all_db_links.db_link%type default null
  ) return clob as
    v_stmt clob;
  begin

    v_stmt := replace (stmt, '${OWNER}s', owner_name);
    v_stmt := replace (v_stmt, '${TABLE}s', table_name);
    v_stmt := replace (v_stmt, '${DB_LINK}s', db_link);

    return v_stmt;
  end fmt_stmt;

begin

  for i in (
    select owner, table_name
    from all_tab_columns
    where owner='BI_PROD'
      and table_name like 'STG_%'
      and column_name='DATA_CENTER_ID'
  ) loop
    dbms_output.put_line('+ '||i.owner||'.'||i.table_name);
    dbms_application_info.set_module (
      module_name => 'MOVING DC '||data_center_id,
      action_name => i.owner||'.'||i.table_name
    );
    begin

-- Delete
      dbms_output.put_line('|+ DELETE');
      dbms_output.put_line('||- START => '||to_char(current_timestamp, 'DD-MON-RRRR HH24:MI:SS'));

      execute immediate
        fmt_stmt (
          stmt => delete_stmt,
          owner_name => i.owner,
          table_name => i.table_name,
          db_link => null
        )
        using data_center_id;

      dbms_output.put_line('||- END => '||to_char(current_timestamp, 'DD-MON-RRRR HH24:MI:SS'));
      dbms_output.put_line('||- SQLROWCOUNT => '||sql%rowcount);

-- Insert
      dbms_output.put_line('|+ INSERT');
      dbms_output.put_line('||- START => '||to_char(current_timestamp, 'DD-MON-RRRR HH24:MI:SS'));

      execute immediate
        fmt_stmt (
          stmt => insert_stmt,
          owner_name => i.owner,
          table_name => i.table_name,
          db_link => source_db_link
        )
        using data_center_id;

      dbms_output.put_line('||- END => '||to_char(current_timestamp, 'DD-MON-RRRR HH24:MI:SS'));
      dbms_output.put_line('||- SQLROWCOUNT => '||sql%rowcount);

-- Commit
      commit;
      dbms_output.put_line('| COMMIT');

-- Exceptions
    exception
      when others then
        dbms_output.put_line('| '||sqlerrm);
        rollback;
        dbms_output.put_line('| ROLLBACK');
    end;

  end loop;

end;
/


