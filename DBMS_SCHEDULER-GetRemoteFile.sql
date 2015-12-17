clear screen;
declare
  v_source_file_unc varchar2(500 char) := '&path_with_file';
  v_credential user_scheduler_credentials.credential_name%type := '&credential_name';
  v_file_contents clob;
  v_line_count number;

  re_pattern constant varchar2(4 char) := '^.+$';
begin

-- Open the temporary clob
  dbms_lob.createtemporary(
    lob_loc => v_file_contents,
    cache => false,
    dur => dbms_lob.session
  );

-- Get the contents
  dbms_scheduler.get_file (
    source_file => v_source_file_unc,
    source_host => null,
    credential_name => v_credential,
    file_contents => v_file_contents
  );

-- Count the number of inserts
  v_line_count := regexp_count(v_file_contents, re_pattern, 1, 'm');

-- Loop throught the inserts
  for i in 1 .. v_line_count
  loop
    declare v_cur_stmt varchar2 (32767 char);
    begin

      -- Get the current statement
      v_cur_stmt := dbms_lob.substr(regexp_substr(v_file_contents, re_pattern, 1, i, 'm'), 32767, 1);

      -- Print the current statement
      dbms_output.put_line(v_cur_stmt);

    end;
  end loop;

exception
  when others then
    raise;
end;
/

