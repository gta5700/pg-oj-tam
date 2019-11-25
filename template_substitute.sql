/*
  GTA 2019-11-14
*/



--  DROP SCHEMA oj_tam CASCADE;
CREATE SCHEMA IF NOT EXISTS oj_tam;

CREATE OR REPLACE FUNCTION oj_tam.template_substitute(p_template text, p_values json, p_open_bracket char DEFAULT '<', p_close_bracket char DEFAULT '>')
RETURNS text AS 
$BODY$
DECLARE TAG_OPEN text = p_open_bracket;
DECLARE TAG_CLOSE text = p_close_bracket;
DECLARE JSON_KEY_CS boolean = TRUE;
DECLARE MAX_INDEX integer;

DECLARE v_curr_index integer;
DECLARE v_tag_start integer;
DECLARE v_raw_start integer;
DECLARE v_char text;

DECLARE v_tag_name text;
DECLARE v_tag_value text;
DECLARE v_raw_text text;
DECLARE v_result text = '';
BEGIN
/*

  SELECT oj_tam.template_substitute(p_template => '->>|Sia³a <BABA> mak <NIE> <WIEDZIA³A> jak|<<-', p_values => '{"<BABA>": "baba", "<NIE>":"nie", "<WIEDZIA³A>": "wiedzia³a"}'::json);

*/
  IF p_template IS NULL THEN /* on Null Null*/
    RETURN NULL;
  END IF;

  IF p_open_bracket IS NULL THEN
    RAISE EXCEPTION  'oj_tam.template_substitute -> p_open_bracket IS NULL'; 
  END IF;
  
  IF p_close_bracket IS NULL THEN
    RAISE EXCEPTION  'oj_tam.template_substitute -> p_close_bracket IS NULL'; 
  END IF;
  
  IF p_open_bracket = p_close_bracket THEN
    RAISE EXCEPTION  'oj_tam.template_substitute -> p_open_bracket = p_close_bracket'; 
  END IF;


  MAX_INDEX = LENGTH(p_template);
  v_curr_index:= 1;
  v_tag_start:= 0;
  v_raw_start:= NULL; 
  v_raw_text:= '';
  v_result:= '';
  WHILE v_curr_index <= MAX_INDEX LOOP
    v_char:= SUBSTRING(p_template FROM v_curr_index FOR 1);

    IF v_char = TAG_OPEN THEN
      /*  wyeskejpowany nawias otwierajacy  */
      IF SUBSTRING(p_template, v_curr_index+1, 1) = TAG_OPEN THEN   
        v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
        v_raw_text:= v_raw_text || TAG_OPEN;
        v_curr_index:= v_curr_index + 2;
        CONTINUE;  
      END IF;

      /* zwracamy raw data, jeœli jest  */
      IF v_raw_start > 0 THEN
        v_result:= v_result || v_raw_text;
        v_raw_text:= '';
        v_raw_start:= NULL;
      END IF;
      
      /* poprawny pocz¹tek TAG-a, szukamy koñca  */
      v_tag_start:= v_curr_index;
      v_curr_index:= v_curr_index + 1;
      WHILE v_curr_index <= MAX_INDEX LOOP 
        v_char:= SUBSTRING(p_template FROM v_curr_index FOR 1);  

        IF v_char = TAG_OPEN THEN
          RAISE EXCEPTION 'pozycja %, znalaz³ nawias otwieraj¹cy w œrodku tag-a,  %', v_curr_index, SUBSTRING(p_template, GREATEST(1, v_curr_index-4), 9);
        END IF;
        
        /* pierwszy nawias zamykajacy  */
        IF v_char = TAG_CLOSE THEN 
          v_tag_name:= SUBSTRING(p_template, v_tag_start, v_curr_index - v_tag_start + 1);  
          IF JSON_KEY_CS THEN
            v_tag_value:= (p_values ->> v_tag_name);
          ELSE
            SELECT tbl.value
            FROM json_each_text(p_values) tbl
            WHERE LOWER(tbl.key) = LOWER(v_tag_name)
            LIMIT 1
            INTO v_tag_value;
          END IF;  

          IF v_tag_value IS NULL THEN
            RAISE EXCEPTION 'oj_tam.template_substitute -> brak wartosci dla taga: %, case sensitive: %',v_tag_name, JSON_KEY_CS;
          END IF;

          v_result:= v_result || v_tag_value;
          v_tag_start:= 0;
          v_curr_index:= v_curr_index + 1;
          EXIT; /*  znalazl koniec TAG-a, wychodzi z pêtli szukaj¹cej konca*/
        ELSE
          /*  jakis znak bedacy czesci¹ TAG-a*/
          v_curr_index:= v_curr_index + 1;
        END IF;
      END LOOP;

      /* */
      IF v_tag_start > 0 THEN 
        RAISE EXCEPTION 'pozycja %, sparsowa³ ca³y napis a nawiasu zamykaj¹cego brak,  %', v_curr_index, SUBSTRING(p_template, GREATEST(1, v_curr_index-4), 9);
      END IF;

    ELSIF v_char = TAG_CLOSE THEN 
      /*  wyeskejpowany nawias zamykaj¹cy */
      IF SUBSTRING(p_template, v_curr_index+1, 1) = TAG_CLOSE THEN 
        v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
        v_raw_text:= v_raw_text || TAG_CLOSE;
        v_curr_index:= v_curr_index + 2;
        CONTINUE;  
      END IF;

      RAISE EXCEPTION 'pozycja %, znalaz³ nawias zamykaj¹cy bez pocz¹tku tag-a,  %', v_curr_index, SUBSTRING(p_template, GREATEST(1, v_curr_index-4), 9);
    ELSE
      /* znaki nie bêd¹ce nawiasami */
      v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
      v_raw_text:= v_raw_text || v_char;
      v_curr_index:= v_curr_index + 1;
    END IF;

  END LOOP;

  
  /* zwracamy raw data, jeœli jest  */
  IF v_raw_start > 0 THEN
    v_result:= v_result || v_raw_text;
  END IF;  

  RETURN v_result;
END
$BODY$
LANGUAGE plpgsql;






