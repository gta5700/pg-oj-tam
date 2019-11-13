/*
  GTA 2019-10-27
  GTA 2019-11-02
*/

DROP FUNCTION IF EXISTS parse_text(p_input text, p_tag_open char, p_tag_close char);
CREATE OR REPLACE FUNCTION parse_text(p_input text, p_tag_open char DEFAULT '<', p_tag_close char DEFAULT '>')
RETURNS TABLE(
  lp integer,
  item_is_tag boolean,  
  item_pretty text,
  item_size integer,
  item_start integer,
  item_end integer,
  item_raw text  
) AS 
$BODY$
DECLARE TAG_OPEN text = p_tag_open;
DECLARE TAG_CLOSE text = p_tag_close;
DECLARE MAX_INDEX integer;
DECLARE v_curr_index integer;
DECLARE v_tag_start integer;
DECLARE v_raw_start integer;
DECLARE v_char text;

DECLARE v_item_raw text;
DECLARE v_item_pretty text;
BEGIN
/*
SELECT * FROM parse_text('siała <ba><<ba>> mak nie <{wiedziała}> jak a <dziad> wiedział nie <powiedział>  '||
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
  v_raw_start:= NULL; 
  v_item_pretty:= '';
  WHILE v_curr_index <= MAX_INDEX LOOP
    v_char:= SUBSTRING(p_input FROM v_curr_index FOR 1);

    IF v_char = TAG_OPEN THEN 
    
      /*  wyeskejpowany nawias otwierajacy  */
      IF SUBSTRING(p_input, v_curr_index+1, 1) = TAG_OPEN THEN   
        v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
        v_item_pretty:= v_item_pretty || TAG_OPEN;
        v_curr_index:= v_curr_index + 2;
        CONTINUE;  
      END IF;

      /* zwracamy raw data, jeśli jest  */
      IF v_raw_start > 0 THEN
        lp:= COALESCE(lp, 0)+1;      
        item_is_tag:= FALSE;   
        item_start:= v_raw_start;
        item_end:= v_curr_index-1;
        item_size:= item_end - item_start + 1;
        
        item_raw:= SUBSTRING(p_input, item_start, item_size);          
        item_pretty:= v_item_pretty;   
        --item:= SUBSTRING(p_input, item_start, item_size);  
        --item_pretty:= v_item_pretty;

        v_item_pretty:= '';
        v_raw_start:= NULL;
        RETURN NEXT;
      END IF;
      
      /* poprawny początek TAG-a, szukamy końca  */
      v_tag_start:= v_curr_index;
      v_curr_index:= v_curr_index + 1;
      WHILE v_curr_index <= MAX_INDEX LOOP 
        v_char:= SUBSTRING(p_input FROM v_curr_index FOR 1);  

        IF v_char = TAG_OPEN THEN
          RAISE EXCEPTION 'pozycja %, znalazł nawias otwierający w środku tag-a,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
        END IF;
        
        /* pierwszy nawias zamykajacy  */
        IF v_char = TAG_CLOSE THEN 
          lp:= COALESCE(lp, 0)+1;      
          item_is_tag:= TRUE;      
          item_start:= v_tag_start;
          item_end:= v_curr_index;
          item_size:= item_end - item_start + 1;
          item_raw:= SUBSTRING(p_input, item_start, item_size);        
          item_pretty:= item_raw;            

          v_tag_start:= 0;
          v_curr_index:= v_curr_index + 1;
          RETURN NEXT;
          EXIT; /*  znalazl koniec TAG-a, wychodzi z pętli szukającej konca*/
        ELSE
          /*  jakis znak bedacy czescią TAG-a*/
          v_curr_index:= v_curr_index + 1;
        END IF;
      END LOOP;

      /* */
      IF v_tag_start > 0 THEN 
        RAISE EXCEPTION 'pozycja %, sparsował cały napis a nawiasu zamykającego brak,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
      END IF;

    ELSIF v_char = TAG_CLOSE THEN 
      /*  wyeskejpowany nawias zamykający */
      IF SUBSTRING(p_input, v_curr_index+1, 1) = TAG_CLOSE THEN 
        v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
        v_item_pretty:= v_item_pretty || TAG_CLOSE;
        v_curr_index:= v_curr_index + 2;
        CONTINUE;  
      END IF;

      RAISE EXCEPTION 'pozycja %, znalazł nawias zamykający bez początku tag-a,  %', v_curr_index, SUBSTRING(p_input, GREATEST(1, v_curr_index-4), 9);
    ELSE
      /* znaki nie będące nawiasami */
      v_raw_start:= COALESCE(v_raw_start, v_curr_index);  
      v_item_pretty:= v_item_pretty || v_char;
      v_curr_index:= v_curr_index + 1;
    END IF;
  END LOOP;

  /* zwracamy raw data, jeśli jest  */
  IF v_raw_start > 0 THEN
    lp:= COALESCE(lp, 0)+1;      
    item_is_tag:= FALSE;   
    item_start:= v_raw_start;
    item_end:= v_curr_index-1;
    item_size:= item_end - item_start + 1;
    item_raw:= SUBSTRING(p_input, item_start, item_size);  
    item_pretty:= v_item_pretty;    
    RETURN NEXT;
  END IF;  
END
$BODY$
LANGUAGE plpgsql;

