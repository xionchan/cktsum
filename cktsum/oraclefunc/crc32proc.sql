CREATE OR REPLACE PACKAGE CALTABCRC32 AS
  FUNCTION S_CRC32(TOWNER IN VARCHAR2, TABLE_NAME IN VARCHAR2, PARTITION_NAME IN VARCHAR2 DEFAULT NULL) RETURN NUMBER;
  FUNCTION P_CRC32(TOWNER IN VARCHAR2, TABLE_NAME IN VARCHAR2, RANGESUM NUMBER DEFAULT NULL, RANGEID NUMBER DEFAULT NULL) RETURN NUMBER;
END CALTABCRC32;
/

CREATE OR REPLACE PACKAGE BODY CALTABCRC32 AS
    function S_CRC32 (TOWNER IN VARCHAR2, TABLE_NAME in varchar2, PARTITION_NAME in varchar2 default null) return number is
        result number DEFAULT 0;
        partstr varchar2(8000);
        getcrc32sql varchar2(20000) default 'select sum(0';
        ifnum number;
        getstrsql varchar2(20000);
begin
EXECUTE IMMEDIATE 'alter session set nls_date_format = ''yyyy-mm-dd hh24:mi:ss''';
EXECUTE IMMEDIATE 'alter session set nls_timestamp_format = ''yyyy-mm-dd hh24:mi:ss.FF''';
EXECUTE IMMEDIATE 'alter session set nls_timestamp_tz_format = ''yyyy-mm-dd hh24:mi:ss.FF''';

-- initial varchar
getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type not in (''CHAR'', ''NUMBER'', ''FLOAT'', ''BLOB'') and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;
IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_STR_CRC(''||listagg(column_name,''||
                    '') WITHIN GROUP (ORDER BY COLUMN_NAME)||'')'' from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' ||
                    TABLE_NAME || ''' and data_type not in (''CHAR'', ''NUMBER'', ''FLOAT'', ''BLOB'') and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            -- initial char, need trim
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
            ''' and data_type = ''CHAR'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;
IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_STR_CRC(trim(''||listagg(column_name,'')||
                    trim('') WITHIN GROUP (ORDER BY COLUMN_NAME)||''))'' from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' ||
                TABLE_NAME || ''' and data_type = ''CHAR'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            /* -- clob
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type = ''CLOB'' and column_name not like ''SYS\_%'' ESCAPE ''\''';
            EXECUTE IMMEDIATE getstrsql into ifnum;
            IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_CLOB_CRC(''||listagg(column_name,'') + CAL_CLOB_CRC('') WITHIN GROUP (ORDER BY column_id)||'')'' from all_tab_cols where owner = ''' || TOWNER ||
                        ''' and table_name = ''' || TABLE_NAME || ''' and data_type = ''CLOB'' and column_name not like ''SYS\_%'' ESCAPE ''\''';
                EXECUTE IMMEDIATE getstrsql into partstr;
                getcrc32sql := getcrc32sql || ' + ' || partstr;
            END IF; */

            -- blob
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type = ''BLOB'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into ifnum;
IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_BLOB_CRC(''||listagg(column_name,'') + CAL_BLOB_CRC('') WITHIN GROUP (ORDER BY column_name)||'')'' from all_tab_cols where owner = ''' || TOWNER ||
                        ''' and table_name = ''' || TABLE_NAME || ''' and data_type = ''BLOB'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            -- number
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type in (''NUMBER'', ''FLOAT'') and column_name not like ''SYS\_%$'' ESCAPE ''\''' ;
EXECUTE IMMEDIATE getstrsql into ifnum;
IF ifnum > 0 THEN
                getstrsql := 'select listagg(column_name,'',0) +
                nvl('') WITHIN GROUP (ORDER BY column_name) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME || ''' and data_type in (''NUMBER'', ''FLOAT'') and column_name not like
''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + nvl(' || partstr || ',0)';
END IF;

            -- partition
            IF PARTITION_NAME IS NULL THEN
                 getcrc32sql := getcrc32sql || ') FROM ' || TOWNER || '.' || TABLE_NAME;
ELSE
                getcrc32sql := getcrc32sql || ') FROM ' || TOWNER || '.' || TABLE_NAME || ' PARTITION (' || PARTITION_NAME || ')';
END IF;

            -- get crc32sum
EXECUTE IMMEDIATE getcrc32sql into result;
return trunc(result);
end S_CRC32;

    function P_CRC32 (TOWNER IN VARCHAR2, TABLE_NAME in varchar2, RANGESUM NUMBER DEFAULT NULL, RANGEID NUMBER DEFAULT NULL) RETURN NUMBER is
        result number DEFAULT 0;
        splitcrc32 number DEFAULT 0;
        getstrsql varchar2(20000);
        partstr varchar2(8000);
        getcrc32sql varchar2(20000) default 'select sum(0';
        ifnum number DEFAULT 0;
cursor cur_rowid IS
SELECT DBMS_ROWID.ROWID_CREATE(1, DATA_OBJECT_ID, RELATIVE_FNO, BLOCK_ID, 0) ROWID1,
       DBMS_ROWID.ROWID_CREATE(1, DATA_OBJECT_ID, RELATIVE_FNO, BLOCK_ID + BLOCKS - 1, 32767) ROWID2
FROM DBA_EXTENTS A, ALL_OBJECTS B
WHERE A.SEGMENT_NAME = B.OBJECT_NAME
  AND A.OWNER = B.OWNER
  AND (A.PARTITION_NAME IS NULL OR A.PARTITION_NAME = B.SUBOBJECT_NAME)
  AND A.OWNER = TOWNER
  AND A.SEGMENT_NAME = TABLE_NAME
  AND MOD(A.EXTENT_ID, RANGESUM) = RANGEID;

BEGIN
EXECUTE IMMEDIATE 'alter session set nls_date_format = ''yyyy-mm-dd hh24:mi:ss''';
EXECUTE IMMEDIATE 'alter session set nls_timestamp_format = ''yyyy-mm-dd hh24:mi:ss.FF''';
EXECUTE IMMEDIATE 'alter session set nls_timestamp_tz_format = ''yyyy-mm-dd hh24:mi:ss.FF''';

-- initial varchar
getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type not in (''CHAR'', ''NUMBER'', ''FLOAT'', ''BLOB'') and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;

IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_STR_CRC(''||listagg(column_name,''||
                    '') WITHIN GROUP (ORDER BY column_name)||'')'' from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' ||
                    TABLE_NAME || ''' and data_type not in (''CHAR'', ''NUMBER'', ''FLOAT'', ''BLOB'') and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            -- char, need trim
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
            ''' and data_type = ''CHAR'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;

IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_STR_CRC(trim(''||listagg(column_name,'')||
                    trim('') WITHIN GROUP (ORDER BY column_name)||''))'' from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' ||
                TABLE_NAME || ''' and data_type = ''CHAR'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            /* -- clob
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type = ''CLOB'' and column_name not like ''SYS\_%'' ESCAPE ''\''';

            EXECUTE IMMEDIATE getstrsql into ifnum;

            IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_CLOB_CRC(''||listagg(column_name,'') + CAL_CLOB_CRC('') WITHIN GROUP (ORDER BY column_id)||'')'' from all_tab_cols where owner = ''' || TOWNER ||
                        ''' and table_name = ''' || TABLE_NAME || ''' and data_type = ''CLOB'' and column_name not like ''SYS\_%'' ESCAPE ''\''';
                EXECUTE IMMEDIATE getstrsql into partstr;
                getcrc32sql := getcrc32sql || ' + ' || partstr;
            END IF;
            */

            -- blob
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type = ''BLOB'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;

IF ifnum > 0 THEN
                getstrsql := 'select ''CAL_BLOB_CRC(''||listagg(column_name,'') + CAL_BLOB_CRC('') WITHIN GROUP (ORDER BY column_name)||'')'' from all_tab_cols where owner = ''' || TOWNER ||
                        ''' and table_name = ''' || TABLE_NAME || ''' and data_type = ''BLOB'' and column_name not like ''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + ' || partstr;
END IF;

            -- number
            getstrsql := 'select count(*) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME ||
                ''' and data_type in (''NUMBER'', ''FLOAT'') and column_name not like ''SYS\_%$'' ESCAPE ''\''';

EXECUTE IMMEDIATE getstrsql into ifnum;

IF ifnum > 0 THEN
                getstrsql := 'select listagg(column_name,'',0) +
                nvl('') WITHIN GROUP (ORDER BY column_name) from all_tab_cols where owner = ''' || TOWNER || ''' and table_name = ''' || TABLE_NAME || ''' and data_type in (''NUMBER'', ''FLOAT'') and column_name not like
''SYS\_%$'' ESCAPE ''\''';
EXECUTE IMMEDIATE getstrsql into partstr;
getcrc32sql := getcrc32sql || ' + nvl(' || partstr || ',0)';
END IF;


            -- rowid
                        getstrsql := getcrc32sql || ') from ' || TOWNER || '.' || TABLE_NAME || ' where rowid between :1 and :2';


                        -- 获取结果
FOR CUR IN cur_rowid LOOP
            EXECUTE IMMEDIATE getstrsql
                INTO splitcrc32 USING CUR.ROWID1, CUR.ROWID2;
            result := result + splitcrc32;
END LOOP;

return trunc(result);
END P_CRC32;
end CALTABCRC32;
/