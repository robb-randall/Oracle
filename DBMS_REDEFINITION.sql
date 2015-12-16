/*
WHY: http://docs.oracle.com/cd/B28359_01/server.111/b28310/tables007.htm#ADMIN01514
HOW: http://docs.oracle.com/cd/B28359_01/appdev.111/b28419/d_redefi.htm#CBBFDJBC
ALTERNATIVES: http://www.toadworld.com/platforms/oracle/w/wiki/4509.tables-rebuilding
*/

set serveroutput on;
set feedback off;
clear screen;

declare
  in_owner all_tables.owner%type := user;
  in_table all_tables.table_name%type := 'ROBB_TEST1';
  in_order_clause varchar2 (32767) := '1 ASC';
  in_use_pk boolean := true;

  v_use_pk number;
  v_int_table varchar2 (30) := 'TMP$'||to_char (sysdate, 'RRRR_MM_DD_HH24MISS');
  v_number_of_errors number;

  procedure println
  (
    msg in varchar2
  ) as
    v_mask varchar2 (26) := 'RRRR-MON-DD HH24:MI:SS';
  begin
    dbms_output.put_line (to_char (sysdate, v_mask)||': '||msg);
  end println;
begin
  println ('START');

-- Redefine by PK column?
  println ('Redefine by:');
  if in_use_pk
    then
      v_use_pk := dbms_redefinition.cons_use_pk;
      println ('PK Column');
    else
      v_use_pk := dbms_redefinition.cons_use_rowid;
      println ('ROWID');
  end if;

-- Check if we can redefine the table
  println ('Can we redfine the table online?');
  begin
    dbms_redefinition.can_redef_table
    (
      uname => in_owner,
      tname => in_table,
      options_flag => v_use_pk,
      part_name => null
    );
    println ('YES');
  exception
    when others then
      println ('NO, exception to follow:');
      raise;
  end;

-- Ordered by
  if in_order_clause is not null then
    println ('Ordered by: '||in_order_clause);
  end if;

-- Interim table
  println ('Interim Table: '||v_int_table);

-- Create Interim table
  println ('Creating Interim Table:');
  execute immediate 'CREATE TABLE '||v_int_table||' AS SELECT * FROM '||in_owner||'.'||in_table||' WHERE 1=2';
  println ('Created');

-- Start the redefine process
  println ('Start Redefinition');
  dbms_redefinition.start_redef_table
  (
    uname => in_owner,
    orig_table => in_table,
    int_table => v_int_table,
    col_mapping => null,
    options_flag => v_use_pk,
    orderby_cols => in_order_clause,
    part_name => null
  );

-- Copy dependecies
  println ('Copy Dependents:');
  dbms_redefinition.copy_table_dependents
  (
    uname => in_owner,
    orig_table => in_table,
    int_table => v_int_table,
    copy_indexes => dbms_redefinition.cons_orig_params,
    copy_triggers => true,
    copy_constraints => false,
    copy_privileges => true,
    ignore_errors => false,
    num_errors => v_number_of_errors,
    copy_statistics => false,
    copy_mvlog => false
  );
  println (v_number_of_errors||' errors encountered.');

-- Sync data
  println ('Syncing Interim Table');
  dbms_redefinition.sync_interim_table
  (
    uname => in_owner,
    orig_table => in_table,
    int_table => v_int_table,
    part_name => null
  );

-- Finish Redefinition
  println ('Finishing Redefinition');
  dbms_redefinition.finish_redef_table
  (
    uname => in_owner,
    orig_table => in_table,
    int_table => v_int_table,
    part_name => null
  );  

-- Remove Interim Table
  println ('Dropping Interim Table:');
  execute immediate 'DROP TABLE '||v_int_table||' PURGE';
  println ('Dropped');

-- Gather Table Statistics
  println ('Gathering Table Stats');
  dbms_stats.gather_table_stats
  (
     ownname => in_owner,
     tabname => in_table,
     partname => null,
     force => false
  );

  println ('END');
exception
  when others then
    println (sqlerrm);

    println ('Abort Redefinition Process');
    dbms_redefinition.abort_redef_table
    (
      uname => in_owner,
      orig_table => in_table,
      int_table => v_int_table,
      part_name => null
    );

    println ('Dropping Interim Table');
    execute immediate 'DROP TABLE '||v_int_table||' PURGE';
end;
/