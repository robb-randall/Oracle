set serveroutput on;
set feedback off;
clear screen;

declare
  in_datapump_logfile varchar2 (255) := 'DEDUPE$EXPORT$RUT15.log';
  in_directory all_directories.directory_name%type := 'DATA_PUMP_DIR';

  v_file_handle utl_file.file_type;
  v_file_buffer varchar2 (32767);

begin

-- Open
  v_file_handle := utl_file.fopen
  (
    location => in_directory,
    filename => in_datapump_logfile,
    open_mode => 'R',
    max_linesize => 32767
  );

-- Read lines
  begin

    loop

      utl_file.get_line
      (
        file => v_file_handle,
        buffer => v_file_buffer,
        len => 32767
      );

      if regexp_like (v_file_buffer, '^\. \. exported "[[:alnum:]_$]+"."[[:alnum:]_$]+"[[:blank:]]+[0-9\.]+ MB[[:blank:]]+[0-9]+ rows') then
        dbms_output.put_line ('SCHEMA ... '||regexp_substr (v_file_buffer, '[^"]+', 1, 2));
        dbms_output.put_line ('TABLE .... '||regexp_substr (v_file_buffer, '[^"]+', 1, 4));
        dbms_output.put_line ('SIZE ..... '||regexp_substr (v_file_buffer, '[0-9]+\.?([0-9]+)?[[:space:]](KB|MB|GB|TB)', 1, 1));
        dbms_output.put_line ('ROWS ..... '||regexp_substr (v_file_buffer, '[0-9]+[[:space:]]rows', 1, 1));
      end if;


      if regexp_like (v_file_buffer, 'ORA-[[:digit:]]+') then
        raise_application_error (-20000, 'Exception encountered during export of data, aborting.');
      end if;

    end loop;

  exception
    when no_data_found then
      dbms_output.put_line ('End of file');
    when others then
      dbms_output.put_line (sqlerrm);
  end;

-- Close file
  utl_file.fclose
  (
    file => v_file_handle
  );

exception
  when others then
    if utl_file.is_open (file => v_file_handle) then
      utl_file.fclose
      (
        file => v_file_handle
      );
    end if;

    raise;
end;
/


