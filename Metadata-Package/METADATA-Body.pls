create or replace package body metadata as

/*******************************************************************************
 * CONVERT_XML_TO_DDL
 */
function convert_xml_to_ddl
(
  object_type in all_objects.object_type%type,
  ddl_xml     in clob
) return clob as
  ddl_clob clob;

  metadata_handle pls_integer;
  metadata_transform_handle pls_integer;

begin

-- Open the metadata handle
  metadata_handle := dbms_metadata.openw
  (
    object_type => object_type
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
    document => ddl_xml,
    result => ddl_clob
  );

-- Close the metadata handle
  dbms_metadata.close
  (
    handle => metadata_handle
  );

  return ddl_clob;
end convert_xml_to_ddl;

/*******************************************************************************
 * GET_OBJECT_DDL
 */
function get_object_ddl
(
  db_link         in user_db_links.db_link%type,
  object_type     in all_objects.object_type%type,
  object_owner    in all_objects.owner%type,
  object_name     in all_objects.object_name%type
) return clob as
  metadata_handle pls_integer;
  ddl_xml clob;
  parsed_items ku$_parsed_items;
  object_type_path varchar2(32767 char);
begin

-- Open metadata object
  metadata_handle := dbms_metadata.open
  (
    object_type => object_type,
    network_link => db_link
  );

-- Set owner filter
  if object_owner is not null then
    dbms_metadata.set_filter
    (
      handle => metadata_handle,
      name => 'SCHEMA',
      value => object_owner
    );
  end if;

-- Set name filter
  dbms_metadata.set_filter
  (
    handle => metadata_handle,
    name => 'NAME',
    value => object_name
  );

-- Create a temporary clob
  dbms_lob.createtemporary
  (
    lob_loc => ddl_xml,
    cache => false
  );

-- Fetch the tablesapce XML
  dbms_metadata.fetch_xml_clob
  (
    handle => metadata_handle,
    doc => ddl_xml,
    parsed_items => parsed_items,
    object_type_path => object_type_path
  );

-- Close the metadata handle
  dbms_metadata.close
  (
    handle => metadata_handle
  );

-- Convert the XML to a DDL statement (CLOB)
  return convert_xml_to_ddl
  (
    object_type => object_type,
    ddl_xml     => ddl_xml
  );

exception
  when others then
    -- If getting the DDL fails, we want to return the error?
    -- If we return the error instead of RAISE, it'll allow us
    -- to capture as much DDL as possible while capturing the errors
    -- we can fix later or ignore.
    return sqlerrm;
end get_object_ddl;

/*******************************************************************************
 * GET_TABLESPACE_DDL
 */
function get_tablespace_ddl
(
  db_link         in user_db_links.db_link%type,
  tablespace_name in user_tablespaces.tablespace_name%type
) return clob as
begin

  return get_object_ddl
  (
    db_link       => db_link,
    object_type   => 'TABLESPACE',
    object_owner  => null,
    object_name   => tablespace_name
  );

end get_tablespace_ddl;

/*******************************************************************************
 * GET_USER_DDL
 */
function get_user_ddl
(
  db_link         in user_db_links.db_link%type,
  username in user_users.username%type
) return clob as
begin

  return get_object_ddl
  (
    db_link       => db_link,
    object_type   => 'USER',
    object_owner  => null,
    object_name   => username
  );

end get_user_ddl;

end metadata;
