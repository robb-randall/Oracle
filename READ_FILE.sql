CREATE OR REPLACE FUNCTION FILE_READ
(
  file in varchar2,
  dir in all_directories.directory_name%type
) return clob as
  file_max_line_size constant number := 32767;
  file_handle utl_file.file_type;
  file_exists boolean;
  file_length number;
  file_block_size binary_integer;
  file_contents clob;
begin

  -- Check if exists
  utl_file.fgetattr
  (
    location => dir,
    filename => file,
    fexists => file_exists,
    file_length => file_length,
    block_size => file_block_size
  );
  
  if not file_exists then
    raise_application_error(-20000, q'{File doesn't exists!}');
  end if;

  -- Open file
  file_handle := utl_file.fopen
  (
    location => dir,
    filename => file,
    open_mode => 'R',
    max_linesize => file_max_line_size
  );
  
  -- Get contents
  <<file_lines_loop>>
  loop
  
    <<line_block>>
    declare line varchar2(32767 char);
    begin
    
      utl_file.get_line
      (
        file => file_handle,
        buffer => line,
        len => file_max_line_size
      );

    file_contents := file_contents || line;

    exception
      when no_data_found then
        exit; -- Exits loop when EOF is reached
      when others then
        raise;
    end line_block;

    file_contents := file_contents || chr(13);

  end loop file_lines_loop;
  
  -- Close file
  utl_file.fclose
  (
    file => file_handle
  );

  -- Return contents
  return file_contents;

exception
  when others then
    if utl_file.is_open(file => file_handle) then
      utl_file.fclose
      (
        file => file_handle
      );
    end if;
    
    raise;
end file_read;
/
