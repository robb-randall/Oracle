set serveroutput on;
set feedback off;
clear screen;

declare
  v_margin_to_fix constant number := 10; -- Number difference to be off before fixing sequence
  v_pad constant number := 25; -- Display padding

begin

  for idx1 in (
    select
      acc.owner,
      acc.table_name,
      acc.column_name,
      aseq.sequence_name
    from all_col_comments acc
    join all_sequences aseq
     on acc.owner=aseq.sequence_owner
     and upper (acc.comments)='SEQ='||aseq.sequence_name
    where owner='&owner'
  ) loop

    declare
      v_max_val number;
      v_current_val number;
      v_stmt clob :=
        'SELECT MAX_ID, '||idx1.owner||'.'||idx1.sequence_name||'.NEXTVAL'||chr(13)||
        'FROM'||chr(13)||
        '('||chr(13)||
        '  SELECT MAX ('||idx1.column_name||') MAX_ID'||chr(13)||
        '  FROM '||idx1.owner||'.'||idx1.table_name||chr(13)||
        ')';
      v_drop_seq clob := 'DROP SEQUENCE '||idx1.owner||'.'||idx1.sequence_name;
      v_create_seq clob;

    begin

      dbms_output.put_line (
        '+ '||idx1.owner||'.'||idx1.table_name||chr(13)||
        rpad ('|- PK COL ', v_pad, '.')||' '||idx1.column_name||chr(13)||
        rpad ('|- SEQ ', v_pad, '.')||' '||idx1.sequence_name
      );

      execute immediate v_stmt
        into v_max_val, v_current_val;

      dbms_output.put_line (
        rpad ('|- MAX ID ', v_pad, '.')||' '||v_max_val||chr(13)||
        rpad ('|- SEQ CURRVAL ', v_pad, '.')||' '||v_current_val
      );

-- Fix the sequence:
      if v_current_val not between v_max_val+1 and (v_max_val+1 + v_margin_to_fix) then

-- Drop sequence
        dbms_output.put (rpad ('|- Dropping Sequence ', v_pad, '.')||' ');

        begin

          execute immediate v_drop_seq;
          dbms_output.put_line ('Sequence dropped.');

        exception
          when others then
            dbms_output.put_line (sqlerrm);
            raise;

        end;

-- Create sequence
        dbms_output.put (rpad ('|- Creating Sequence ', v_pad, '.')||' ');

        v_create_seq := 'CREATE SEQUENCE '||idx1.owner||'.'||idx1.sequence_name||' START WITH '||to_char (v_max_val+1);

        begin

          execute immediate v_create_seq;
          dbms_output.put_line ('Sequence created.');

        exception
          when others then
            dbms_output.put_line (sqlerrm);
        end;

      end if;

    exception
      when others then
        dbms_output.put_line (v_stmt);
        raise;

    end;

  end loop;

end;
/

