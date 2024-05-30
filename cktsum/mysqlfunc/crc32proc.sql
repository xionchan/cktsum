---------------MySQL----------------------
CREATE PROCEDURE calcrc32fortable(
    TABLE_SCHEMA VARCHAR(50),
    TABLE_NAME VARCHAR(50),
    PARTITION_NAME VARCHAR(50),
    OUT CRC32SUM BIGINT
)
BEGIN
    -- initial sql header
    SET group_concat_max_len = 102400;
    SET @getcrc32sql = 'select sql_no_cache sum(0';

    -- initial varchar
    SET @presql = concat('select count(*) into @ifnum from information_schema.columns where table_name =''', TABLE_NAME, ''' AND table_schema = ''', TABLE_SCHEMA, ''' AND
        NUMERIC_PRECISION is null and data_type <> ''char'' and data_type not like ''%blob'' and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED'')))');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

IF @ifnum > 0 THEN
        SET @presql = concat('select concat(''crc32(concat(ifnull(`'',GROUP_CONCAT(COLUMN_NAME order by COLUMN_NAME collate utf8_general_ci SEPARATOR ''`,"")
                ,ifnull(`''),''`,"")))'') into @strlist from information_schema.columns where table_name = ''', TABLE_NAME, ''' AND table_schema = ''',TABLE_SCHEMA, ''' AND
                NUMERIC_PRECISION is null and data_type <> ''char'' and data_type not like ''%blob'' and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED'')))');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

SET @getcrc32sql = concat(@getcrc32sql, ' + ', @strlist);
END IF;

    -- initial char
    SET @presql = concat('select count(*) into @ifnum from information_schema.columns where table_name = ''', TABLE_NAME, ''' AND table_schema = ''', TABLE_SCHEMA, ''' AND
             data_type = ''char''  and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED''))) ');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

IF @ifnum > 0 THEN
        SET @presql = concat('select concat(''crc32(concat(ifnull(`'',GROUP_CONCAT(COLUMN_NAME order by COLUMN_NAME collate utf8_general_ci SEPARATOR ''`,"")
            ,ifnull(`''),''`,"")))'') into @charlist from information_schema.columns where table_name = ''', TABLE_NAME, ''' AND table_schema = ''',TABLE_SCHEMA, ''' AND
            data_type = ''char'' and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED''))) ');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

SET @getcrc32sql = concat(@getcrc32sql, ' + ', @charlist);
END IF;

    -- initial BLOB
    SET @presql = concat('select count(*) into @ifnum from information_schema.columns where table_name = ''', TABLE_NAME, ''' AND table_schema = ''', TABLE_SCHEMA, ''' AND
                    data_type like ''%blob'' and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED''))) ');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

IF @ifnum > 0 THEN
        SET @presql = concat('select concat(''ifnull(crc32(`'', GROUP_CONCAT(COLUMN_NAME order by COLUMN_NAME collate utf8_general_ci SEPARATOR ''`),0) + ifnull(crc32(`''),''`),0)'')
                            into @loblist from information_schema.columns where table_name = ''', TABLE_NAME, ''' AND table_schema = ''',TABLE_SCHEMA,
                            ''' AND data_type like ''%blob'' and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED'')))');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

SET @getcrc32sql = concat(@getcrc32sql, ' + ', @loblist);
END IF;

    -- initial number
    SET @presql = CONCAT('select count(*) into @ifnum from information_schema.columns where TABLE_NAME = ''', TABLE_NAME, ''' and TABLE_SCHEMA = ''',TABLE_SCHEMA,
                    ''' and NUMERIC_PRECISION is not null and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED'')))');
PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

IF @ifnum > 0 THEN
        SET @presql = concat('select concat(''ifnull(`'',GROUP_CONCAT(COLUMN_NAME order by COLUMN_NAME collate utf8_general_ci SEPARATOR ''`,0) +
                                ifnull(`''),''`,0)'') into @numlist from information_schema.columns where table_name = ''', TABLE_NAME, ''' and table_schema = ''',
                                TABLE_SCHEMA,''' and NUMERIC_PRECISION is not null and not (column_key = ''PRI'' and (extra in (''auto_increment'', ''DEFAULT_GENERATED'')))');

PREPARE dynamic_statement FROM @presql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

SET @getcrc32sql = concat(@getcrc32sql, ' + ', @numlist);
END IF;

    -- initial partition
    IF PARTITION_NAME IS NOT NULL THEN
        SET @getcrc32sql = concat(@getcrc32sql, ') into @crc32andsum from ', TABLE_SCHEMA, '.', TABLE_NAME, ' PARTITION(', PARTITION_NAME, ')');
ELSE
        SET @getcrc32sql = concat(@getcrc32sql, ') into @crc32andsum from ', TABLE_SCHEMA, '.', TABLE_NAME);
END IF;

    -- get result
PREPARE dynamic_statement FROM @getcrc32sql;
EXECUTE dynamic_statement;
DEALLOCATE PREPARE dynamic_statement;

SET CRC32SUM = truncate(@crc32andsum, 0);
END;
//