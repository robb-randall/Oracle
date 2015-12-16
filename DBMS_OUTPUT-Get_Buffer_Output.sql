create table output_buffer
(
  session_sid number,
  lineno number,
  line varchar2 (4000 char)
);
/


create or replace procedure dump_buffer
as
  buffer_text dbmsoutput_linesarray;
  lines number := 32767;
  session_sid number := sys_context('userenv','sid');
begin

  dbms_output.get_lines(buffer_text, lines);
  dbms_output.put_line(lines);
  for line in 1 .. lines --buffer_text.count
  loop
    insert into output_buffer (session_sid, lineno, line)
      values (session_sid, to_number(line), substr(buffer_text(line),1,4000));
  end loop;

  commit;

end dump_buffer;
/
show errors


set serveroutput off;
begin
  dbms_output.enable(); -- Must be enabled to work
  for i in 1 .. 100
  loop
    dbms_output.put_line('LINE => '||i);
  end loop;
  dump_buffer(); -- Captures the output from enable.
end;
/


-- truncate table output_buffer;
SELECT * FROM output_buffer;

