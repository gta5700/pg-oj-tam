
/*
  GTA 2019-11-13
*/
CREATE SCHEMA IF NOT EXISTS oj_tam;


DROP TABLE IF EXISTS oj_tam.szablony_numeracji_tagi;
DROP TABLE IF EXISTS oj_tam.szablony_numeracji;



CREATE TABLE oj_tam.szablony_numeracji(
  id serial NOT NULL PRIMARY KEY,
  kod varchar(64) NOT NULL UNIQUE,
  nazwa varchar(64),
  maska text,
  func_name text,
  opis text
);


CREATE TABLE oj_tam.szablony_numeracji_tagi(
  id serial NOT NULL PRIMARY KEY,
  fk_sznu integer NOT NULL,
  tag varchar(64) NOT NULL UNIQUE,
  opis text,
  sql_statement text,
  callback_data json NOT NULL DEFAULT '{}'::json
);



DROP FUNCTION IF EXISTS generuj_wartosc_taga(p_id_parameru_01 integer, p_callback_data json);
CREATE OR REPLACE FUNCTION generuj_wartosc_taga(p_tag text, p_id_parameru_01 integer, p_callback_data json, OUT tag_value text, OUT callback_data json)
RETURNS record AS 
$BODY$
/*
  SELECT * FROM generuj_wartosc_taga('<CURRENT_YEAR>', 1, NULL);
  SELECT * FROM generuj_wartosc_taga('<PREVIOUS_YEAR>', 1, NULL);
  SELECT * FROM generuj_wartosc_taga('<NEXT_YEAR>', 1, NULL);
  SELECT * FROM generuj_wartosc_taga('<CURRENT_DATE>', NULL, NULL);
  SELECT * FROM generuj_wartosc_taga('<XXX.YYY>', NULL, NULL);
*/
BEGIN

  /* prymitywna bazówka, ignorująca p_id_parameru_01 i p_callback_data */
  IF p_tag = '<CURRENT_YEAR>' THEN 
    tag_value:= CAST(date_part('year', CURRENT_DATE) AS text);
    callback_data:= p_callback_data;
    RETURN;
    
  ELSIF p_tag = '<PREVIOUS_YEAR>' THEN 
    tag_value:= CAST(date_part('year', CURRENT_DATE)-1 AS text);
    callback_data:= p_callback_data;
    RETURN;
    
  ELSIF p_tag = '<NEXT_YEAR>' THEN 
    tag_value:= CAST(date_part('year', CURRENT_DATE)+1 AS text);
    callback_data:= p_callback_data;
    RETURN;
    
  ELSIF p_tag = '<CURRENT_DATE>' THEN 
    tag_value:= to_char(CURRENT_DATE, 'YYYY-MM-DD');
    callback_data:= p_callback_data;
    RETURN;
    
  ELSIF p_tag = '<CURRENT_USER>' THEN 
    tag_value:= (SELECT CURRENT_USER::text);
    callback_data:= p_callback_data;
    RETURN;
    
  ELSE
    RAISE EXCEPTION 'generuj_wartosc_taga(p_tag => %, p_id_parameru_01 => %, p_callback_data => %) -> nieznana\nieobsługiwana wartosc tag-a',p_tag, p_id_parameru_01, p_callback_data;
  END IF;
  
END;
$BODY$
LANGUAGE plpgsql;





INSERT INTO oj_tam.szablony_numeracji(kod, nazwa, func_name, maska)
VALUES ('oj_tam.dokumenty', 'generator dla dokumentów', 'generuj_wartosc_taga', 
        'UMPAUMPA - <LPAD(8, ''0'')><CURRENT_YEAR>@<CURRENT_USER>');

INSERT INTO oj_tam.szablony_numeracji_tagi(fk_sznu, tag, opis, sql_statement)
VALUES ((SELECT MAX(id) FROM oj_tam.szablony_numeracji), '<CURRENT_USER>',
        '',
        'SELECT * FROM generuj_wartosc_taga(''<CURRENT_USER>'', $1, $2)'
      );

INSERT INTO oj_tam.szablony_numeracji_tagi(fk_sznu, tag, opis, sql_statement)
VALUES ((SELECT MAX(id) FROM oj_tam.szablony_numeracji), '<CURRENT_YEAR>',
        '',
        'SELECT * FROM generuj_wartosc_taga(''<CURRENT_YEAR>'', $1, $2)'
      );




DROP FUNCTION IF EXISTS oj_tam.generuj_numer(); 
DROP FUNCTION IF EXISTS oj_tam.generuj_numer(p_id_szablonu integer, p_id_parameru_01 integer, p_id_parameru_02 text);
CREATE OR REPLACE FUNCTION oj_tam.generuj_numer(p_id_szablonu integer, p_id_parameru_01 integer 
        /* p_preview_only boolean DEFAULT NULL,  p_skip_locking boolean DEFAULT NULL */) 
RETURNS text AS 
$BODY$

DECLARE v_szablon record;
DECLARE v_tag record;
DECLARE v_tag_values jsonb;
DECLARE v_tag_value text;
DECLARE v_tag_exec_result record; 
DECLARE v_result text;
DECLARE v_callback_data json;
DECLARE v_clock_timeout timestamp;
DECLARE v_lock_aquired boolean;
DECLARE v_func_data record;
DECLARE CNST_WAIT_TIMEOUT_SEC integer = 1.5 + 5.0 + 13.5; 

DECLARE v_regexp text;
DECLARE RE_FUNKCJE text[] = ARRAY[
    '^\<\s*(LPAD)\(\s*(\d+)\s*\,\s*\''(.{1})\''\s*\)\s*\>$',
    '^\<\s*(IF_EMPTY)\(\s*\''(.*)\''\s*\)\s*\>$',
    '^\<\s*(IF_NOT_EMPTY_A)\(\s*\''(.*)\''\s*\)\s*\>$',
    '^\<\s*(IF_NOT_EMPTY_B)\(\s*\''(.*)\''\s*\)\s*\>$'
    ];

BEGIN
  --  RAISE NOTICE 'oj_tam.generuj_numer(p_id_szablonu => %, p_id_parameru_01 => %)', p_id_szablonu, p_id_parameru_01;
  
  IF NOT EXISTS (SELECT * FROM oj_tam.szablony_numeracji WHERE id = p_id_szablonu) THEN 
    RAISE EXCEPTION 'oj_tam.generuj_numer -> brak szablonu po podanym id = %',p_id_szablonu;
  END IF;

  IF p_id_parameru_01 IS NULL THEN 
    RAISE NOTICE 'oj_tam.generuj_numer -> p_id_parameru_01 = NULL';
  END IF;

  /*  próbujemy zalozyc LOCK-a na szablon  */
  v_lock_aquired:= FALSE;
  v_clock_timeout = clock_timestamp() + make_interval(secs => CNST_WAIT_TIMEOUT_SEC);
  WHILE NOT v_lock_aquired AND clock_timestamp()  <= v_clock_timeout LOOP
    --  RAISE NOTICE 'proba zalozenia locka -> % < %', clock_timestamp(), v_clock_timeout;
    BEGIN
        SELECT * FROM oj_tam.szablony_numeracji WHERE id = p_id_szablonu 
        INTO v_szablon 
        FOR UPDATE NOWAIT;
        
        IF v_szablon.id IS NOT NULL THEN
            v_lock_aquired:= TRUE;
            EXIT;
        END IF;
    EXCEPTION
        WHEN LOCK_NOT_AVAILABLE /* 55P03 */ THEN
          --  RAISE NOTICE 'still waiting';
        WHEN OTHERS THEN
          RAISE;
    END;
    
    PERFORM pg_sleep(0.25) WHERE NOT v_lock_aquired;
  END LOOP;

  IF NOT v_lock_aquired THEN 
    RAISE EXCEPTION 'oj_tam.generuj_numer -> nie udalo się uzyskać LOCK-a';
  END IF;

  v_result:= '';
  v_tag_values:= '{}'::json;

  SELECT FALSE as jest_ok, ''::text as func_name
  INTO v_func_data;
  
   FOR v_tag IN 
      SELECT tagi.id as fk_tag, tagi.callback_data, parse.item_is_tag, parse.item_pretty, tagi.sql_statement, szab.func_name 
      FROM oj_tam.szablony_numeracji szab,
        parse_text(szab.maska, '<', '>') parse
        LEFT JOIN oj_tam.szablony_numeracji_tagi tagi ON (tagi.tag = parse.item_pretty AND parse.item_is_tag)
      WHERE szab.id = p_id_szablonu
      ORDER BY parse.lp ASC
  LOOP 
    --RAISE NOTICE 'oj_tam.generuj_numer -> v_tag = %', v_tag.item_pretty;

    /* trafil raw text, mimo ze v_func_data jest zainicjowane i powinna byc funkcja */
    IF NOT v_tag.item_is_tag AND v_func_data.jest_ok THEN 
      RAISE EXCEPTION 'oj_tam.generuj_numer() -> trafil raw text: %s, mimo że oczekiwal taga dla funkcji', v_tag.item_pretty;
    END IF;

    /* trafil raw text, dodaje do wyniku i idzie do kolejnego itema-a */
    IF NOT v_tag.item_is_tag THEN 
      v_result:= v_result || v_tag.item_pretty;
      CONTINUE;
    END IF;

    /* czy tag jest funkcją, test na tag jest nadmiarowy  */
    -- RAISE NOTICE 'szuka %, v_tag.item_is_tag = %, v_func_data.jest_ok  = %', v_tag.item_pretty, v_tag.item_is_tag, v_func_data.jest_ok;  
    IF v_tag.item_is_tag AND v_func_data.jest_ok IS DISTINCT FROM TRUE THEN

        FOREACH v_regexp IN ARRAY RE_FUNKCJE LOOP
          SELECT TRUE as jest_ok, regx[1] as func_name,  regx[2] as param_01, regx[3] as param_02, regx
          FROM regexp_matches(v_tag.item_pretty, v_regexp) regx
          INTO v_func_data;
 
          EXIT WHEN v_func_data.jest_ok; /* pętla po RE_FUNKCJE */
        END LOOP;

        IF v_func_data.jest_ok THEN
          /*  znalazł funkcje   */
          -- RAISE NOTICE 'znalazl funkcję: v_func_data = %', v_func_data;  
          CONTINUE; /* pętla po tag-ach*/
        END IF;
    END IF;

    IF v_tag.fk_tag IS NULL THEN 
      RAISE EXCEPTION 'oj_tam.generuj_numer -> nieznany\ nieobsługiwany tagg: %', v_tag.item_pretty;
    END IF;

    /* brak defaultowej funkcji dla szablonu i detailowego sql na poziomie tag-a*/
    IF COALESCE(v_tag.func_name, '') = '' AND COALESCE(v_tag.sql_statement, '') = '' THEN 
      RAISE EXCEPTION 'oj_tam.generuj_numer -> brak funkcji\sql do wygenerowania wartoci tag-a: %',v_tag.item_pretty;
    END IF;

    /* jesli tag jest uzyty w masce kilkakrotnie, to szukamy w juz wygenerowanych tag-ach*/
    v_tag_value:= v_tag_values ->> v_tag.item_pretty::text;

    IF v_tag_value IS NULL THEN 
      IF COALESCE(v_tag.sql_statement, '') <> '' THEN
        /*  sql detailowy na poziomie taga  */
        --RAISE NOTICE 'oj_tam.generuj_numer -> %: custom sql', v_tag.item_pretty;
        EXECUTE v_tag.sql_statement
        INTO v_tag_exec_result
        USING p_id_parameru_01, v_tag.callback_data;
      ELSE
        /* sql domyslny zdefniowany na poziomie szablonu */
        --RAISE NOTICE 'oj_tam.generuj_numer -> %: default sql', v_tag.item_pretty;
        EXECUTE FORMAT('SELECT * FROM %s($1, $2, $3)', v_tag.func_name)
        INTO v_tag_exec_result
        USING v_tag.item_pretty, p_id_parameru_01, v_tag.callback_data;
      END IF;

      /*  jesli dostaniemy NULL-e to maskujemy */
      v_tag_exec_result.tag_value:= COALESCE(v_tag_exec_result.tag_value, '');
      v_tag_exec_result.callback_data:= COALESCE(v_tag_exec_result.callback_data, '{}'::json);
      
      /* podreczny bakap wygenerowanej wartosci, każda wartosc tag-a jest generowana TYLKO RAZ */
      v_tag_values:= jsonb_set(v_tag_values::jsonb, ARRAY[v_tag.item_pretty]::text[], to_jsonb(v_tag_exec_result.tag_value));
     
      UPDATE oj_tam.szablony_numeracji_tagi SET
        callback_data = v_tag_exec_result.callback_data
      WHERE id = v_tag.fk_tag;
    END IF;


    --  RAISE NOTICE 'v_func_data: %', v_func_data; 
    IF v_func_data.jest_ok AND v_func_data.func_name = 'LPAD' THEN
      /* LPAD(ile = xxx, znak = 'x' ) */
      v_result:= v_result || REPEAT(CAST(v_func_data.param_02 AS char), GREATEST(0, CAST(v_func_data.param_01 AS integer) - LENGTH(v_tag_exec_result.tag_value))) || v_tag_exec_result.tag_value;
      v_func_data.jest_ok:= FALSE;

    ELSIF v_func_data.jest_ok AND v_func_data.func_name = 'IF_EMPTY' THEN    
      /* jesli v_tag_exec_result.tag_value; to wstawiamy wartosc v_func_data.param_01 */
      v_result:= v_result || COALESCE(NULLIF(v_tag_exec_result.tag_value, ''), v_func_data.param_01);
      v_func_data.jest_ok:= FALSE; 
      
    ELSIF v_func_data.jest_ok AND v_func_data.func_name = 'IF_NOT_EMPTY_A' THEN 
      /* doklejać A-fter*/  
      v_result:= v_result || v_tag_exec_result.tag_value || (CASE WHEN v_tag_exec_result.tag_value <> '' THEN v_func_data.param_01 ELSE '' END); 
      v_func_data.jest_ok:= FALSE;
        
    ELSIF v_func_data.jest_ok AND v_func_data.func_name = 'IF_NOT_EMPTY_B' THEN 
      /* doklejać B-efore*/  
      v_result:= v_result || (CASE WHEN v_tag_exec_result.tag_value <> '' THEN v_func_data.param_01 ELSE '' END) || v_tag_exec_result.tag_value; 
      v_func_data.jest_ok:= FALSE;        
    ELSE
    
      v_result:= v_result || v_tag_exec_result.tag_value;
    END IF;
    
  END LOOP;

  --RAISE NOTICE 'oj_tam.generuj_numer -> result: %',v_result; 
  /* ewentualnie dorzucić v_tag_values  */
  RETURN v_result;  
END;
$BODY$
LANGUAGE plpgsql;



 SELECT oj_tam.generuj_numer(
    p_id_szablonu => (SELECT MAX(id) FROM oj_tam.szablony_numeracji), 
    p_id_parameru_01 => NULL::integer
 );
     