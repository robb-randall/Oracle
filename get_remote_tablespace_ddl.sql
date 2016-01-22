/*******************************************************************************
This script uses DBMS_METADATA to get remote tablespace DDL using database links

select
  get_remote_tablesapces_ddl (db_link => 'MY_DB_LINK', tablespace_name => 'USERS') tblsp_ddl
from dual;

*******************************************************************************/
create or replace function get_remote_tablesapces_ddl
(
  db_link         in user_db_links.db_link%type,
  tablespace_name in user_tablespaces.tablespace_name%type
) return clob is
  metadata_handle number;
  metadata_transform_handle number;
  xml_clob clob;
  ddl_clob clob;
  parsed_items ku$_parsed_items;
  object_type_path varchar2(32767 char);
begin

/*******************************************************************************
 * This part gets the remote tablespace info in XML format
 */

-- Open metadata object
  metadata_handle := dbms_metadata.open
  (
    object_type => 'TABLESPACE',
    network_link => db_link
  );

-- Set filter to the tablesapce name
  dbms_metadata.set_filter
  (
    handle => metadata_handle,
    name => 'NAME',
    value => tablespace_name
  );

-- Create a temporary clob
  dbms_lob.createtemporary
  (
    lob_loc => xml_clob,
    cache => false
  );

-- Fetch the tablesapce XML
  dbms_metadata.fetch_xml_clob
  (
    handle => metadata_handle,
    doc => xml_clob,
    parsed_items => parsed_items,
    object_type_path => object_type_path
  );

-- Close the metadata handle
  dbms_metadata.close
  (
    handle => metadata_handle
  );

/*******************************************************************************
 * This part converts the XML document into the DDL (and should be
 * separated from the above section if/when put into a package) 
 */

-- Open the metadata handle
  metadata_handle := dbms_metadata.openw
  (
    object_type => 'TABLESPACE'
  );

-- Add the DDL transform
  metadata_transform_handle := dbms_metadata.add_transform
  (
    handle => metadata_handle,
    name => 'DDL'
  );

-- Create a temporary clob
  dbms_lob.createtemporary
  (
    lob_loc => ddl_clob,
    cache => false
  );

-- Convert the XML to DDL
  dbms_metadata.convert (
    handle => metadata_handle,
    document => xml_clob,
    result => ddl_clob
  );

-- Close the metadata handle
  dbms_metadata.close
  (
    handle => metadata_handle
  );

-- Retrun DDL
  return ddl_clob;

exception
  when others then
    dbms_output.put_line(sqlerrm);
    raise;
end get_remote_tablesapces_ddl;
/
