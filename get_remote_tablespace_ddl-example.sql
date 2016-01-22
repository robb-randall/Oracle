set serveroutput on;
clear screen;

/* cleanup:
-- truncate table tablespaces_ddl;
select * from tablespaces_ddl;
*/

<<main>>
begin

  <<db_links_loop>>
  for i in
  (
    select db_link
    from user_db_links
  )
  loop
    dbms_output.put(rpad(i.db_link||' ', 36, '.')||' ');

    <<exec_insert>>
    begin
      execute immediate
        'insert into tablespaces_ddl
        (
          database_link_name,
          host_name,
          instance_name,
          rowdate,
          tablespace_name,
          tablespace_ddl
        )
        select
          ''' || i.db_link || ''' as database_link_name,
          host_name,
          instance_name,
          sysdate as rowdate,
          tablespace_name,
          get_remote_tablesapces_ddl (db_link => ''' || i.db_link || ''', tablespace_name => tablespace_name) tablespace_ddl
        from v$instance@'|| i.db_link ||' i, dba_tablespaces@'|| i.db_link ||'';

      dbms_output.put_line(sql%rowcount||' records inserted.');

    exception
      when others then
        dbms_output.put_line(sqlerrm);
    end exec_insert;

    commit; -- Commit transaction and close db link

  end loop db_links_loop;

end main;
/
