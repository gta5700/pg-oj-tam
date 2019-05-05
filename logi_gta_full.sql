/*
  2019-0X-0X old, partion
*/
DROP SCHEMA IF EXISTS logi_gta CASCADE;
CREATE SCHEMA logi_gta;


CREATE SEQUENCE logi_gta.seq_logi_master;
CREATE TABLE logi_gta.logi_master
(
  id bigint NOT NULL DEFAULT nextval(('logi_gta.seq_logi_master'::text)::regclass),
  kiedy timestamp,
  kod_operacji char,
  id_tabeli_data bigint,
  primary_key_value text,
  txid bigint,
  trigger_depth integer,
  login character varying(64),
  id_pracownika bigint,
  id_tabeli_log bigint,
  partition_check date,
  detail_data jsonb,
  CONSTRAINT logi_master_pkey PRIMARY KEY (id)
);


CREATE SEQUENCE logi_gta.seq_logi_detail;
CREATE TABLE logi_gta.logi_detail
(
  id bigint NOT NULL DEFAULT nextval(('logi_gta.seq_logi_detail'::text)::regclass),
  id_logow_master bigint,
  kolumna character varying(64),
  bylo text,
  jest text,
  id_tabeli_data bigint,
  partition_check date,
  CONSTRAINT logi_detail_pkey PRIMARY KEY (id)
);

CREATE SEQUENCE logi_gta.seq_tabele;
CREATE TABLE logi_gta.tabele
(
  id bigint NOT NULL DEFAULT nextval(('logi_gta.seq_tabele'::text)::regclass),
  schemat character varying(64),
  nazwa character varying(64),
  primary_key_name character varying(64),
  full_name character varying(130),
  typ character(1),
  detail_full_name character varying(130),
  CONSTRAINT tabele_pkey PRIMARY KEY (id)
);



CREATE OR REPLACE FUNCTION logi_gta.auto_log_trigger_function()
RETURNS trigger AS
$BODY$
DECLARE v_log_id bigint;
DECLARE v_old_value text;
DECLARE v_new_value text;
DECLARE v_pk_value text;
DECLARE v_kolumna record;
DECLARE v_log_table_name text;

DECLARE v_tabela_log logi_gta.tabele%rowtype;
DECLARE v_tabela_data logi_gta.tabele%rowtype;
DECLARE v_json jsonb;
DECLARE v_json_old json;
DECLARE v_json_new json;
BEGIN
	--  nie ma przekierowywania logów to tabel dedykowanych dla konkretnej tabeli
	--  liczy sie data zdarzenia
	SELECT  *
	FROM logi_gta.tabele
	WHERE full_name = 'logi_gta.logi_master_' || TRANSLATE(CAST(CURRENT_DATE as text), '-', '_')
	INTO v_tabela_log;

	IF v_tabela_log.id IS NULL THEN	
		SELECT * 
		FROM logi_gta.init_new_log_table(CURRENT_DATE)
		INTO v_tabela_log; 
	END IF;


  --  tabela danych 
	SELECT  *
	FROM logi_gta.tabele
	WHERE schemat = TG_TABLE_SCHEMA AND nazwa = TG_TABLE_NAME
	INTO v_tabela_data;

	IF v_tabela_data.id IS NULL THEN	
		SELECT * 
		FROM logi_gta.init_new_log_table(CAST(TG_TABLE_SCHEMA AS varchar), CAST(TG_TABLE_NAME AS varchar))
		INTO v_tabela_data; 
	END IF;

	--	v_result.primary_key_name; v_pk_value
	IF COALESCE(v_tabela_data.primary_key_name, '') <> '' THEN
	
		IF TG_OP IN ('UPDATE', 'DELETE') THEN	
			EXECUTE 'SELECT ($1::'|| v_tabela_data.full_name ||').'|| v_tabela_data.primary_key_name 
			INTO v_pk_value USING OLD;			
		ELSIF TG_OP IN ('INSERT') THEN
			EXECUTE 'SELECT ($1::'|| v_tabela_data.full_name ||').'|| v_tabela_data.primary_key_name 
			INTO v_pk_value USING NEW;				
		END IF;   
	ELSE
		v_pk_value:= NULL;	
	END IF;

  v_json_old:= NULL;
  v_json_new:= NULL;
  
  IF TG_OP IN ('UPDATE', 'DELETE') THEN	
    EXECUTE 'SELECT row_to_json($1::'|| v_tabela_data.full_name ||') '
    INTO v_json_old 
    USING OLD;	
  END IF;

  IF TG_OP IN ('UPDATE', 'INSERT') THEN	
    EXECUTE 'SELECT row_to_json($1::'|| v_tabela_data.full_name ||') '
    INTO v_json_new 
    USING NEW;	
  END IF;

  --RAISE NOTICE 'v_json_old -> %',v_json_old;
  --RAISE NOTICE 'v_json_new -> %',v_json_new;

  --  zapisujemy tylko róznice chyba że jest INSERT lub DELETE, wtedy wszystko
  --  GTA -> przerobic: indywidualne SQL dla INSERT, UPDATE, DELETE, 
  --  zeby nie zapisaywac bylo NULL i jest NULL dla INSERT i DELETE
  EXECUTE  'SELECT  jsonb_object_agg(j_key, j_values) 
            FROM (
              SELECT COALESCE(json_old.key, json_new.key) as j_key, 
                     json_build_array(json_old.value, json_new.value) as j_values
              FROM json_each_text( $1 ) json_old
                   FULL OUTER JOIN 
                   json_each_text( $2 ) json_new ON (json_old.key = json_new.key)
              WHERE (json_old.value IS DISTINCT FROM json_new.value) 
                OR $3 IN (''INSERT'', ''DELETE'' )
            ) tbl'
  INTO v_json
  USING v_json_old, v_json_new, TG_OP;
	 
	--	wstawiamy od razu do docelowej tabeli
	EXECUTE 'INSERT INTO ' || v_tabela_log.full_name ||'(kiedy, kod_operacji, id_tabeli_data, primary_key_value, txid, trigger_depth, login, id_pracownika, id_tabeli_log, partition_check, detail_data) 
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
           RETURNING id' 
	INTO v_log_id
	USING clock_timestamp(), left(TG_OP, 1), v_tabela_data.id, v_pk_value, txid_current(), pg_trigger_depth(), NULL, podaj_id_uzytkownika(), v_tabela_log.id, CURRENT_TIMESTAMP, v_json;

  /*  STARY ZAPIS DO TABELI detail, docelowo do usuniecia */
	--	(OLD.* <> NEW.*)
	FOR v_kolumna IN 
		SELECT att.attname 
        FROM pg_class cls
			JOIN pg_attribute att ON (att.attrelid = cls.oid)
			JOIN pg_namespace nms ON (nms.oid = cls.relnamespace)
        WHERE 	cls.relname = v_tabela_data.nazwa  
				AND nms.nspname = v_tabela_data.schemat 
				AND att.attnum > 0 
				AND NOT att.attisdropped 
	LOOP  
		v_old_value:= NULL;
		v_new_value:= NULL;
		
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
      EXECUTE 'SELECT ($1::'|| v_tabela_data.full_name ||').'|| v_kolumna.attname INTO v_old_value USING OLD;		
    END IF;
    
    IF TG_OP IN ('UPDATE', 'INSERT') THEN
      EXECUTE 'SELECT ($1::'|| v_tabela_data.full_name ||').'|| v_kolumna.attname INTO v_new_value USING NEW;
    END IF;       
 
    --	pomij jesli podczas UPDATE i nie zmieniono wartosci kolumny
    CONTINUE WHEN (TG_OP = 'UPDATE' AND (v_new_value IS NOT DISTINCT FROM v_old_value));
 
    --	RAISE NOTICE 'old: %, new: %, eval: %',v_old_value, v_new_value,(v_old_value IS NOT DISTINCT FROM v_new_value);
    EXECUTE 'INSERT INTO ' || v_tabela_log.detail_full_name ||'(id_logow_master, kolumna, bylo, jest, id_tabeli_data, partition_check) 
             VALUES ($1, $2, $3, $4, $5, $6); '
    USING v_log_id, v_kolumna.attname, v_old_value, v_new_value, v_tabela_data.id, CURRENT_DATE; 

	END LOOP; 

	--	??
	RETURN NULL; 
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION logi_gta.init_new_data_tables(p_init_tables text[])
RETURNS void AS
$BODY$
DECLARE v_tabela record;
DECLARE v_skip_tables text[] = ARRAY['public.%',
										'information_schema.%',
										'pg_catalog.%',
										'logi_gta.%'
										];
DECLARE v_init_tables text[] = ARRAY[]::text[];										
BEGIN
	v_skip_tables:= v_skip_tables || (SELECT ARRAY_AGG(CAST(full_name AS text)) FROM logi_gta.tabele);
	v_init_tables:= v_init_tables || p_init_tables;
	
	--  RAISE NOTICE 'skip tables ----------> %', v_skip_tables;  
	--  RAISE NOTICE ' tables ----------> %', v_init_tables;  

	FOR v_tabela IN

		SELECT nms.nspname || '.' || cls.relname as schemat_tabela, nms.nspname as schemat, cls.relname as tabela 
		FROM pg_class cls
			JOIN pg_namespace nms ON (nms.oid = cls.relnamespace)
		WHERE cls.relkind = 'r'
			AND NOT (nms.nspname || '.' || cls.relname ILIKE ANY(v_skip_tables))
			AND (nms.nspname || '.' || cls.relname ILIKE ANY(v_init_tables))
	LOOP
		--	EXECUTE 'DROP TRIGGER IF EXISTS zzz_auto_logowanie ON '|| v_tabela.schemat_tabela;

    PERFORM logi_gta.init_new_log_table(v_tabela.schemat, v_tabela.tabela);
	END LOOP;

END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION logi_gta.init_new_log_table(p_partition_date date)
RETURNS SETOF logi_gta.tabele AS
$BODY$
DECLARE v_result logi_gta.tabele%rowtype;
BEGIN
 
 --  tabela child logow master do zapisu logów, dla childow dedaili nie tworzymy
	v_result.id:= nextval('logi_gta.seq_tabele');
	v_result.schemat:= 'logi_gta';
	v_result.nazwa:= 'logi_master_' || TRANSLATE(CAST(p_partition_date AS text), '-', '_');
	v_result.primary_key_name:= 'id';	
	v_result.full_name:= v_result.schemat || '.' || v_result.nazwa;
	v_result.detail_full_name:= 'logi_gta.logi_detail_' || TRANSLATE(CAST(p_partition_date AS text), '-', '_');
	v_result.typ:= 'L'; --  tabelka w której zapisujemy logi
	
	-- pomijamy testy
	INSERT INTO logi_gta.tabele 
	SELECT v_result.*;
	
	--	MASTER 
	EXECUTE 'CREATE TABLE ' || v_result.full_name || '(
				CHECK ( partition_check = CAST(''' || CAST(p_partition_date AS text) || '''AS date) ),
				PRIMARY KEY (id)
			) INHERITS (logi_gta.logi_master)';
			
	
	EXECUTE 'CREATE INDEX ON ' || v_result.full_name || ' (id_tabeli_data);';
	EXECUTE 'CREATE INDEX ON ' || v_result.full_name || ' (id_tabeli_log);';


	--	DETAIL 
	EXECUTE 'CREATE TABLE ' || v_result.detail_full_name || '(
				CHECK ( partition_check = CAST(''' || CAST(p_partition_date AS text) || '''AS date) ),
				PRIMARY KEY (id)
			) INHERITS (logi_gta.logi_detail)';


  RETURN QUERY SELECT v_result.*;   
END
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION logi_gta.init_new_log_table(p_schema_name text, p_table_name text)
RETURNS SETOF logi_gta.tabele AS
$BODY$
DECLARE v_result logi_gta.tabele%rowtype;
BEGIN
 
 --  tabela i informatecją o tabeli ktorej zawartosc logujemy
	v_result.id:= nextval('logi_gta.seq_tabele');
	v_result.schemat:= p_schema_name;
	v_result.nazwa:= p_table_name;
	v_result.primary_key_name:= 'id';	
	v_result.full_name:= v_result.schemat || '.' || v_result.nazwa;
  v_result.typ:= 'D'; --  tabelka która zawiera Dane do logowania

	--	SELECT (CASE WHEN (count(*) OVER (PARTITION BY cls.oid)) = 1 THEN att.attname ELSE NULL END)::varchar as klucz_glowny
	--	idx.indnatts liczba kolumn w indexie
	SELECT CAST(att.attname AS varchar) as klucz_glowny
	FROM pg_class cls
		JOIN pg_index idx ON (idx.indrelid = cls.oid)
		JOIN pg_attribute att ON (att.attrelid = cls.oid AND att.attnum = ANY(idx.indkey))
	WHERE cls.relkind = 'r'
		AND cls.oid = v_result.full_name::regclass
		AND idx.indisprimary 
		AND idx.indisunique
		AND idx.indnatts = 1 
	INTO v_result.primary_key_name;
	
	-- pomijamy testy
	INSERT INTO logi_gta.tabele 
	SELECT v_result.*;


  RETURN QUERY SELECT v_result.*;   
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION logi_gta.utworz_triggery(p_connect_tables text[], p_skip_tables text[] DEFAULT NULL::text[])
RETURNS void AS
$BODY$
DECLARE v_tabela record;
DECLARE v_skip_tables text[] = ARRAY['public.%',
										'information_schema.%',
										'pg_catalog.%',
										'logi_gta.%'
										];
DECLARE v_connect_tables text[] = ARRAY[]::text[];										
BEGIN
	v_skip_tables:= v_skip_tables || p_skip_tables;
	v_connect_tables:= v_connect_tables || p_connect_tables;
	
	--RAISE NOTICE 'skip tables ----------> %', v_skip_tables;  
	--RAISE NOTICE 'connect tables ----------> %', v_connect_tables;  

	PERFORM logi_gta.usun_triggery();
	
	FOR v_tabela IN

		SELECT nms.nspname || '.' || cls.relname as schemat_tabela, nms.nspname as schemat, cls.relname as tabele 
		FROM pg_class cls
			JOIN pg_namespace nms ON (nms.oid = cls.relnamespace)
		WHERE cls.relkind = 'r'
			AND NOT (nms.nspname || '.' || cls.relname ILIKE ANY(v_skip_tables))
			AND (FALSE
          OR (v_connect_tables[1] IS NULL)
          OR (nms.nspname || '.' || cls.relname ILIKE ANY(v_connect_tables))
          )
	LOOP
  	--	EXECUTE 'DROP TRIGGER IF EXISTS zzz_auto_logowanie ON '|| v_tabela.schemat_tabela;    
		EXECUTE 'CREATE TRIGGER zzz_auto_logowanie AFTER INSERT OR UPDATE OR DELETE ON ' || 
				     v_tabela.schemat_tabela || '  ' ||
				    'FOR EACH ROW EXECUTE PROCEDURE logi_gta.auto_log_trigger_function();';         
                    
	END LOOP;

END
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION logi_gta.usun_triggery(p_silent_mode boolean DEFAULT TRUE)
RETURNS void AS
$BODY$
DECLARE v_tabela record;
DECLARE v_setting text;
--	pomijamy TYLKO tabele systemowe
DECLARE v_skip_tables text[] = ARRAY['public.%',
										'information_schema.%',
										'pg_catalog.%'
										];
BEGIN

  IF p_silent_mode THEN
    SELECT setting FROM pg_settings WHERE name = 'client_min_messages'
    INTO v_setting;
    SET client_min_messages TO ERROR;
  END IF;

	FOR v_tabela IN
		SELECT nms.nspname || '.' || cls.relname as schemat_tabela, nms.nspname as schemat, cls.relname as tabele 
		FROM pg_class cls
			JOIN pg_namespace nms ON (nms.oid = cls.relnamespace)
		WHERE cls.relkind = 'r'       
		       AND NOT (nms.nspname || '.' || cls.relname ILIKE ANY(v_skip_tables))
	LOOP

		EXECUTE 'DROP TRIGGER IF EXISTS zzz_auto_logowanie ON '|| v_tabela.schemat_tabela;  
		-- RAISE NOTICE '----------> %',v_tabela.schemat_tabela;  

	END LOOP;

  IF p_silent_mode THEN
    UPDATE pg_settings SET setting = v_setting WHERE name = 'client_min_messages';
  END IF;


END
$BODY$
LANGUAGE plpgsql;

  

CREATE OR REPLACE FUNCTION logi_gta.show_log_master(IN p_data date, 
  OUT id bigint,
  OUT kiedy timestamp,
  OUT full_name varchar(130),
  OUT pk_name varchar(64),
  OUT pk_value text,
  OUT kod_operacji character(1),
  OUT txid bigint, 
  OUT trigger_depth integer
  )
RETURNS SETOF record AS 
$BODY$
BEGIN

  RETURN QUERY 
    SELECT  lm.id, lm.kiedy, tb.full_name, 
        tb.primary_key_name AS _pk_name, lm.primary_key_value AS _pk_value, 
        lm.kod_operacji, lm.txid, lm.trigger_depth
    FROM logi_gta.logi_master lm
      LEFT JOIN logi_gta.tabele tb ON (tb.id = lm.id_tabeli_data)
    WHERE lm.partition_check = p_data
    ORDER BY lm.id;
    
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION logi_gta.show_log_detail(IN p_data date, 
  OUT id bigint,
  OUT lp bigint,
  OUT kiedy timestamp,
  OUT full_name varchar(130),
  OUT pk_name varchar(64),
  OUT pk_value text,
  OUT kod_operacji character(1),
  OUT txid bigint, 
  OUT trigger_depth integer, 
  OUT kolumna varchar(64),
  OUT bylo text,
  OUT jest text
  )
RETURNS SETOF record AS 
$BODY$
BEGIN

  RETURN QUERY 
    SELECT lm.id, COALESCE(ld.id, 0::bigint) AS _lp, lm.kiedy,
        tb.full_name, tb.primary_key_name AS _pk_name, lm.primary_key_value AS _pk_value,
        lm.kod_operacji,lm.txid, lm.trigger_depth,
        ld.kolumna, ld.bylo, ld.jest
    FROM logi_gta.logi_master lm
      LEFT JOIN logi_gta.tabele tb ON (tb.id = lm.id_tabeli_data)
      LEFT JOIN logi_gta.logi_detail ld ON (ld.id_logow_master = lm.id AND ld.partition_check = p_data)
    WHERE lm.partition_check = p_data
    ORDER BY lm.id, ld.id;
    
END
$BODY$
LANGUAGE plpgsql;


  
DROP VIEW IF EXISTS logi_gta.show_log;
CREATE OR REPLACE VIEW logi_gta.show_log AS 
SELECT * FROM logi_gta.show_log_detail( CURRENT_DATE );



SELECT * FROM logi_gta.init_new_data_tables(ARRAY['%.%']);
SELECT * FROM logi_gta.utworz_triggery(ARRAY['%.%']);


SELECT * FrOM logi_gta.show_log;
