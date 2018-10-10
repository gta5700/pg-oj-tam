
CREATE OR REPLACE FUNCTION populate_record(p_table_data anyelement)
RETURNS anyelement AS
$BODY$
/*
  SELECT * FROM populate_record(NULL::some_schema.some_table);
  SELECT * FROM populate_record(NULL::fakt.faktury);  

*/
DECLARE v_result record;
DECLARE v_sql text;
BEGIN
  --RAISE NOTICE 'GTA populate_record -> %',pg_typeof(p_table_data);

  WITH tabelka AS (
    SELECT  pg_namespace.nspname as schema_name, pg_class.relname as table_name,
            pg_attribute.attnum as col_number, pg_attribute.attname as col_name,
            pg_attrdef.adsrc AS default_value, pg_type.typname as typ_raw, typ_format,
            format('CAST(%s AS %s)', COALESCE(pg_attrdef.adsrc, 'NULL'), typ_format) as sql_part,
            pg_attribute.attrelid, pg_typeof(pg_attribute.attrelid),
            pg_class.reltype, pg_class.reloftype
    FROM  pg_namespace
        JOIN pg_class ON (pg_class.relnamespace = pg_namespace.oid)
        JOIN pg_attribute ON (pg_attribute.attrelid = pg_class.oid) 
        JOIN pg_type ON (pg_type.oid = pg_attribute.atttypid)
        JOIN LATERAL format_type(pg_attribute.atttypid, pg_attribute.atttypmod) typ_format ON (TRUE)
        LEFT JOIN pg_attrdef ON (pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum )
    WHERE TRUE
      AND pg_class.relkind IN ('r', 'v')
      AND pg_class.reltype =  pg_typeof(p_table_data)::oid
      AND pg_attribute.attnum > 0
      AND NOT pg_attribute.attisdropped
    ORDER BY pg_attribute.attnum
  )
  SELECT STRING_AGG(tabelka.sql_part, ', ' ORDER BY tabelka.col_number)
  FROM tabelka
  INTO v_sql;

  --RAISE NOTICE 'GTA populate_record.VALUES() -> %',v_sql;
  
  IF COALESCE(v_sql, '') = '' THEN
     v_result:= NULL;
  ELSE
    EXECUTE format('VALUES(%s)', v_sql)
    INTO v_result;
  END IF;

  RETURN v_result;
END  
$BODY$
LANGUAGE plpgsql;


