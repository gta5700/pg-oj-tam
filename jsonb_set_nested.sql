/*  GTA 2019-11-02  */

CREATE OR REPLACE FUNCTION jsonb_set_nested(data jsonb, path text[], new_value jsonb)
RETURNS jsonb AS
$BODY$
DECLARE
  chk_path text[];
  cur_path text[];
  cur_idx text;
  cur_value jsonb;
  def_obj jsonb default '{}'::jsonb;
BEGIN
  /*
  https://postgrespro.com/list/thread-id/2174532
  */
  chk_path := path[1:array_length(path, 1) - 1];
  
  IF (data #> chk_path IS NULL) THEN  -- fast check
    FOREACH cur_idx IN ARRAY chk_path LOOP
      cur_path := cur_path || cur_idx;
      cur_value = data #> cur_path;

      IF (cur_value IS NULL) THEN
        data = jsonb_set(data, cur_path, def_obj);
      ELSIF (jsonb_typeof(cur_value) NOT IN ('object', 'array')) THEN
        RAISE EXCEPTION 'path element by % is neither object nor array', cur_path;
      END IF;
    END LOOP;
  ELSIF (jsonb_typeof(data #> chk_path) NOT IN ('object', 'array')) THEN
      RAISE EXCEPTION 'path element by % is neither object nor array', chk_path;
  END IF;
  
  RETURN jsonb_set(data, path, new_value);
END
$BODY$
LANGUAGE plpgsql;
