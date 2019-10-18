/*
  GTA 2019-10-18
*/


CREATE OR REPLACE FUNCTION parse_text(p_input text, p_tag_open char DEFAULT '<', p_tag_close char DEFAULT '>')
RETURNS TABLE(
  lp integer,
  tag_name text,
  tag_size integer,
  tag_start integer,
  tag_end integer
) AS 
$BODY$
DECLARE TAG_OPEN text = p_tag_open;
DECLARE TAG_CLOSE text = p_tag_close;
DECLARE MAX_INDEX integer;
DECLARE v_curr_index integer;
DECLARE v_tag_start integer;
DECLARE v_tag_end integer;
DECLARE v_item text;
BEGIN
/*


SELECT * FROM parse_text('siała <ba><ba> mak nie <{wiedziała}> jak a <dziad> wiedział nie <powiedział>  '||
                         'a to było tak: było morze w morzu kołek a ten kołek miał <wierzchołek>, '||
                         'na wierzchołku siedział zając o <nożkami> przebierając śpiewał tak',
                         '<', '>');

SELECT * FROM parse_text('siała {ba}<ba> mak nie <wie{dz}iała> jak a {dzIAd} wiedział {{{}}}nie <powiedział>', '{', '}');

SELECT * FROM parse_text(NULL, '{', '}');
*/

  IF p_input IS NULL THEN
    RETURN;
  END IF;

  IF p_tag_open IS NULL THEN
    RAISE EXCEPTION  'pg_temp.parse_text -> p_tag_open IS NULL'; 
  END IF;
  
  IF p_tag_close IS NULL THEN
    RAISE EXCEPTION  'pg_temp.parse_text -> p_tag_close IS NULL'; 
  END IF;
  
  IF p_tag_open = p_tag_close THEN
    RAISE EXCEPTION  'pg_temp.parse_text -> p_tag_open = p_tag_close'; 
  END IF;

  MAX_INDEX = LENGTH(p_input);
  v_curr_index:= 1;
  v_tag_start:= 0;
  v_tag_end:= 0; 
  WHILE v_curr_index <= MAX_INDEX LOOP
  
    v_item:= SUBSTRING(p_input FROM v_curr_index FOR 1);
    /*  RAISE NOTICE 'v_curr_index = % => %',v_curr_index, v_item;  */

   
    IF v_item = TAG_OPEN THEN
    
      IF SUBSTRING(p_input, v_curr_index+1, 1) = TAG_OPEN THEN 
        /*  poprawnie wyeskejpowany nawias otwierajacy */
        v_curr_index:= v_curr_index + 2;
        CONTINUE;  
      END IF;

      v_tag_start:= v_curr_index;
      v_curr_index:= v_curr_index + 1;
      WHILE v_curr_index <= MAX_INDEX LOOP 
          v_item:= SUBSTRING(p_input FROM v_curr_index FOR 1);

          IF v_item = TAG_CLOSE THEN 
            /*  poprawny nawias zamykający */
            v_tag_end:= v_curr_index;
            lp:= COALESCE(lp, 0)+1;            
            tag_start:= v_tag_start;
            tag_end:= v_tag_end;
            tag_size:= v_tag_end - v_tag_start+1;
            tag_name:= SUBSTRING(p_input, tag_start, tag_size);
            
            v_tag_start:= 0;
            v_tag_end:= 0; 
            v_curr_index:= v_curr_index + 1;
            RETURN NEXT;
            EXIT; /* z pętli */
          ELSIF v_item = TAG_OPEN THEN 
            RAISE EXCEPTION 'pozycja %, znalazł nawias otwierający w środku tag-a,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
          ELSE
            v_curr_index:= v_curr_index + 1;
          END IF;          
      END LOOP;

      IF v_tag_start > v_tag_end THEN 
        RAISE EXCEPTION 'pozycja %, sparsował cały napis a nawiasu zamykającego brak,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
      END IF;
      
    ELSIF v_item = TAG_CLOSE THEN

        IF SUBSTRING(p_input, v_curr_index+1, 1) = TAG_CLOSE THEN 
          /*  poprawnie wyeskejpowany nawias zamykający */
          v_curr_index:= v_curr_index + 2;
          CONTINUE;  
        ELSE
           RAISE EXCEPTION 'pozycja %, znalazł nawias zamykający bez początku tag-a,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
        END IF;

    ELSE
      v_curr_index:= v_curr_index + 1;  
    END IF;

  END LOOP; 

END
$BODY$
LANGUAGE plpgsql;



