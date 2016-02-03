CREATE OR REPLACE PACKAGE BODY TRADINGANALYSIS.TP_REPLICATE_DLAKE
IS


   FUNCTION err_stack
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN    'Error_Stack...'
             || CHR (10)
             || DBMS_UTILITY.format_error_stack
             || CHR (10)
             || 'Error_Backtrace...'
             || CHR (10)
             || DBMS_UTILITY.format_error_backtrace;
   END;
   
  

 

  

   PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2, p_type IN VARCHAR2)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      c_s_err     VARCHAR2 (10) := 'ERROR:';
      c_s_info    VARCHAR2 (10) := '';
      v_message   message_log.MESSAGE%TYPE;
      user_name   VARCHAR2 (60);
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'OS_USER') INTO user_name FROM DUAL;

      IF p_type = 'ERROR'
      THEN
         v_message := c_s_err || p_message || err_stack;
      ELSE
         v_message := c_s_info || p_message;
      END IF;

      INSERT INTO message_log (TIME, MESSAGE, USERID)
           VALUES (SYSDATE, v_message, user_name);

      COMMIT;
   END;
   
    procedure exec_sql(v_text in varchar2)
   is
   begin
      
      begin   
      log_message('Before Execute Sql Text: '||v_text,'info');
      execute immediate v_text;
      log_message('Executed SQL Statement: '||v_text||' rows processed:'||sql%rowcount,'info');
         exception when others
         then
            log_message('Execution error Sql Text: '||v_text || ' '|| err_stack,'error');
            raise;
      end;
      
   end;
   
   PROCEDURE gather_stats(in_table_name in varchar2) 
   IS
   BEGIN
      
            log_message ('COLLECT ORACLE STATS:' || in_table_name, 'info');

            SYS.DBMS_STATS.GATHER_TABLE_STATS (
               OwnName            => 'TRADINGANALYSIS',
               TabName            => in_table_name,
               Estimate_Percent   => 2,
               Method_Opt         => 'FOR ALL COLUMNS SIZE 1',
               Degree             => 4,
               Cascade            => TRUE,
               No_Invalidate      => FALSE);
      
   END;
   
   
   
   procedure create_table(p_owner in varchar2, p_src_table_name in varchar2, p_dest_table_name in varchar2,p_db_link  in varchar2, p_data_source in varchar2)
   is
   v_sql varchar2(2000);
   
   begin
   
    
      v_sql:='create table '||p_dest_table_name || ' as select * from '||p_owner||'.'||p_src_table_name || '@'||p_db_link || ' where 1=0'; 
      EXECUTE IMMEDIATE v_sql;
      log_message('Executed SQL Statement: '||v_sql,'info');
      
      update TRADINGANALYSIS.TP_DLAKE_SNAPSHOT_TABLES set SNAPSHOT_START_time=sysdate
      where src_table_name = p_src_table_name and db_link = p_db_link and data_source = p_data_source;
      
      exception when others then 
         log_message(err_stack,'error');  
         raise; 
      
   end;
   
   procedure drop_table(p_dest_table_name in varchar2)
   is
   v_sql varchar2(100);
   begin
      v_sql:='drop table '||p_dest_table_name;
      log_message('Execute SQL Statement: '||v_sql,'info');
      begin
         execute immediate v_sql;
         exception when others then
            null;
      end;
   end;
   
   procedure load_table(p_owner in varchar2, p_src_table_name in varchar2, p_dest_table_name in varchar2,p_db_link  in varchar2, p_mode in varchar2 default 'append', p_data_source in varchar2)
   is
   v_sql varchar2(100);
   v_append_hint varchar2(30):= '/*+ '||p_mode || '*/';
   begin
   
      
      v_sql:='insert '||v_append_hint || ' into '||p_dest_table_name || 
      ' select * from '||p_owner||'.'||p_src_table_name || '@'||p_db_link;
      exec_sql(v_sql); 
      
      update TRADINGANALYSIS.TP_DLAKE_SNAPSHOT_TABLES set SNAPSHOT_finish_time=sysdate, 
      SNAPSHOT_DURATION=to_number(nvl(SNAPSHOT_finish_time,sysdate)-nvl(SNAPSHOT_START_time,'01-jan-2000'))*1440
      where src_table_name = p_src_table_name and db_link = p_db_link and data_source = p_data_source;
      commit;
      
      exception when others then 
         log_message(err_stack,'error');   
         raise;
      
   end;
   
   procedure create_tables(p_data_source in varchar2, p_db_link in varchar2)
   is
   begin
      for rec in (select DATA_SOURCE, DB_LINK, OWNER, SRC_TABLE_NAME, DEST_TABLE_NAME||g_staging_table_suffix DEST_TABLE_NAME
      from TRADINGANALYSIS.TP_DLAKE_SNAPSHOT_TABLES where UPPER(active)='Y'  and data_source=p_data_source and db_link = p_db_link)
      loop
         drop_table(rec.dest_table_name);
         create_table(p_owner=>rec.owner, p_src_table_name =>rec.src_table_name, p_dest_table_name =>rec.dest_table_name,p_db_link=>rec.db_link, 
         p_data_source=>rec.data_source);
             
      end loop;
       exception when others then 
         log_message(err_stack,'error');   
   end; 
   
   
   procedure create_job(p_job_name in varchar2,p_action in varchar2, p_start_time in date default sysdate) 
   is
   begin
      
   SYS.DBMS_SCHEDULER.CREATE_JOB
    (
       job_name        => g_schema||'.'||p_job_name
      ,start_date      => TO_TIMESTAMP_TZ(to_char(sysdate+1,'yyyy/mm/dd hh24:mi:ss')||'.000000 Australia/Sydney','yyyy/mm/dd hh24:mi:ss.ff tzr')
      ,repeat_interval => null
      ,end_date        => NULL
      ,job_class       => 'DEFAULT_JOB_CLASS'
      ,job_type        => 'PLSQL_BLOCK'
      ,job_action      => p_action
      ,comments        => 'full snapshot of '||p_job_name
    );
    
     
     
   
   end;
   
   
   procedure load_tables(p_data_source in varchar2, p_db_link in varchar2, p_fast in boolean default false)
   is
   v_job_action varchar2(300);
  
   begin
   
      
      for rec in (select DATA_SOURCE, DB_LINK, OWNER, SRC_TABLE_NAME, DEST_TABLE_NAME||g_staging_table_suffix DEST_TABLE_NAME
      from TRADINGANALYSIS.TP_DLAKE_SNAPSHOT_TABLES where UPPER(active)='Y' and data_source=p_data_source and db_link = p_db_link)
      loop
         if p_fast = false then
            load_table(p_owner=>rec.owner, p_src_table_name =>rec.src_table_name, p_dest_table_name =>rec.dest_table_name,p_db_link=>rec.db_link,p_data_source=>rec.data_source);
         else
         -- parallel it using Oracle jobs to make it faster
            v_job_action:= 'begin ' || g_program||'.load_table(P_OWNER =>'||q||rec.owner||q||',P_SRC_TABLE_NAME=>'||q||rec.src_table_name||q||',P_DEST_TABLE_NAME=>'
            ||q||rec.dest_table_name||q||',P_DB_LINK=>'||q||rec.db_link||q||',P_DATA_SOURCE=>'||q||rec.data_source||q||');' || ' end;' ;
            log_message(v_job_action,'info');
            create_job(rec.dest_table_name||'_JOB',v_job_action,sysdate);
         end if;
             
      end loop;
       exception when others then 
         log_message(err_stack,'error');   
         
   end;
   
--   procedure create_view(p_table_name in varchar2)
--   is
--   v_prefix varchar2(10):= 'V_TP_';
--   v_sql_header varchar2(200):= ' create or replace view '||v_prefix || p_table_name || ' ';
--   v_view_col_list varchar2(5000);
--   v_sql_text varchar2(3000);
--   v_convert varchar2(50);
--   v_table_name varchar2(32);
--   v_row_limit varchar2(32):=' where rownum <= 1000 ';
--    
--   begin
--   
--   -- create view and ensure column order is the same as the staging table
--   -- if there is any column that failed data type validation then leave the column as varchar2
--      for rec in (
--        with get_columns as (
--        select  st.src_table_name, t.table_name,c.column_name field_name,column_id from user_tables t join user_tab_columns c
--        on t.table_name=c.table_name
--        join   TRADINGANALYSIS.v_TP_DLAKE_SNAPSHOT_TABLES st 
--        on t.table_name = st.DEST_full_table_name
--        where st.active='Y' and st.src_table_name = p_table_name
--        order by st.src_table_name,column_id
--        ),
--        get_conv as (
--        select a.src_table_name, a.table_name dest_table_name,a.field_name,b.field_length,b.data_type,b.data_format,b.ORACLE_TYPE,a.column_id
--        from 
--        get_columns a LEFT join TRADINGANALYSIS.V_TP_DLAKE_TYPE b
--        on UPPER(a.src_table_name) = UPPER(b.table_name) and UPPER(a.field_name) = UPPER(b.field_name) 
--        order by dest_table_name,column_id)
--        select c.src_table_name, c.dest_table_name,c.field_name,c.field_length,c.data_type,c.data_format,c.ORACLE_TYPE,c.column_id,v.validation_error 
--        from get_conv c left join         
--        TRADINGANALYSIS.TP_DLAKE_TABLE_VALIDATION v
--        on c.src_table_name = v.table_name and c.field_name = v.field_name
--        order by dest_table_name,column_id
--      )
--      loop
--      
--     v_table_name:=rec.dest_table_name;
--          
--     if rec.validation_error is null then   
--         if REC.ORACLE_TYPE like 'NUMBER%' then
--            v_convert:= 'TO_NUMBER(TRIM('|| REC.field_name || '))';
--         elsif REC.ORACLE_TYPE like 'DAT%' THEN 
--            v_convert:='TO_DATE(TRIM('||REC.field_name||'),'||q||rec.data_format||q||')';
--         else
--            v_convert:='TRIM('|| REC.field_name || ')';
--         end if;
--     else
--     -- if data_type validation failed then use the original data_type
--        v_convert:= REC.field_name;    
--     end if;
--     
--     
--         
--      
--      
--      if rec.column_id = 1 then
--      
--         v_sql_text:= ' select '||v_convert || ' '|| REC.FIELD_NAME || ' ';  
--         v_view_col_list:= '('|| rec.field_name ;
--      else
--         v_sql_text:= v_sql_text || ',' || v_convert || ' '|| REC.FIELD_NAME;
--      --   v_view_col_list:= v_view_col_list || ','|| rec.field_name ;   
--      end if;  
--      
--      
--            
--      end loop;
--      
--    --  v_view_col_list:= v_view_col_list || ') ';
--       v_sql_text:= v_sql_text || ' from ' || v_table_name || v_row_limit || ';';
--   
--   
--   dbms_output.put_line(v_sql_text);
--   
--   
--   
--   end;
--   
   
    /* Formatted on 02/02/2016 10:08:48 AM (QP5 v5.269.14213.34769) */
PROCEDURE transform_table (p_src_table_name   IN VARCHAR2, p_dest_table_name   IN VARCHAR2)
IS

-- the code assume p_src_table_name is the source table and p_dest_table_name is the final table
   
   v_sql_text     VARCHAR2 (32767);
   v_create_sql varchar2(50);
   v_convert      VARCHAR2 (100);
   v_table_name   VARCHAR2 (100);
   v_staging_table_name varchar2(50):= p_src_table_name || g_staging_table_suffix;
BEGIN
   -- create view and ensure column order is the same as the staging table
   -- if there is any column that failed data type validation then leave the column as varchar2
   FOR rec
      IN (WITH get_columns
               AS (  SELECT 
                            p_src_table_name src_table_name,
                            p_dest_table_name dest_table_name,
                            t.table_name staging_table_name,
                            c.column_name field_name,
                            column_id,
                            c.data_type original_data_type
                       FROM user_tables t
                            JOIN user_tab_columns c
                               ON t.table_name = c.table_name
                            where t.table_name = v_staging_table_name
                   ORDER BY c.column_id),
               get_conv
               AS (  SELECT a.src_table_name,
                            a.staging_table_name,
                            a.dest_table_name,
                            a.field_name,
                            a.original_data_type,
                            b.field_length,
                            b.data_type,
                            b.data_format,
                            b.ORACLE_TYPE target_data_type,
                            a.column_id
                       FROM get_columns a
                            LEFT JOIN TRADINGANALYSIS.V_TP_DLAKE_TYPE b
                               ON     UPPER (a.src_table_name) =
                                         UPPER (b.table_name)
                                  AND UPPER (a.field_name) =
                                         UPPER (b.field_name)
                   ORDER BY dest_table_name, column_id)
            SELECT c.src_table_name,
                   c.DEST_TABLE_NAME,
                   c.field_name,
                   c.field_length,
                   c.data_type,
                   c.data_format,
                   c.original_data_type,
                   nvl(c.target_data_type,'NA') target_data_type,
                   c.column_id,
                   trim(v.validation_error) validation_error
              FROM get_conv c
                   LEFT JOIN TRADINGANALYSIS.TP_DLAKE_TABLE_VALIDATION v
                      ON     c.DEST_TABLE_NAME = v.table_name
                         AND c.field_name = v.field_name
          ORDER BY dest_table_name, column_id)
   LOOP
      v_create_sql:='create table '||rec.dest_table_name || ' as ';
      v_table_name:=rec.dest_table_name;
      IF rec.validation_error IS NULL
      THEN
         IF rec.original_data_type LIKE 'VARCHAR%'   -- only convert if original data type is varchar2
         THEN
            IF REC.target_data_TYPE LIKE 'NUMBER%'
            THEN
               v_convert := 'TO_NUMBER(TRIM(' || REC.field_name || ')) ' ||REC.field_name ;
            ELSIF REC.target_data_TYPE LIKE 'DAT%'
            THEN
               v_convert :=
                     'TO_DATE(TRIM('
                  || REC.field_name
                  || '),'
                  || q
                  || rec.data_format
                  || q
                  || ') ' ||REC.field_name;
            ELSE
               v_convert := REC.field_name  ;    -- leave the original type as we don't know what type to convert
            END IF;
         ELSE
            v_convert := REC.field_name;   -- leave the original type as it is strongly typed
         END IF;
      ELSE
         -- if data_type validation failed then use the original data_type
         v_convert := REC.field_name;
      END IF;



      IF rec.column_id = 1
      THEN
         v_sql_text := 'select ' || v_convert;
      ELSE
         v_sql_text := v_sql_text || ', ' || v_convert ;
      END IF;
   END LOOP;
   
   if length(v_sql_text) > 0 then

   --  v_view_col_list:= v_view_col_list || ') ';
      v_sql_text := v_sql_text || ' from '|| v_table_name || ' where rownum<= 20000;';
   
      v_sql_text:= v_create_sql||v_sql_text;
   else
      log_message('No SQL Statement created','error');
   end if;
   
  -- exec_sql(v_sql_text);
  log_message(substr(v_sql_text,1,400),'info');
  
  execute immediate v_sql_text;


   DBMS_OUTPUT.put_line (v_sql_text);
   DBMS_OUTPUT.put_line ('');
   DBMS_OUTPUT.put_line ('');
END;
   
   
   
   
     procedure validate_data(p_staging_table_name in varchar2,p_check_rows in number default 100)
   is
   v_prefix varchar2(10):= 'V_TP_';
   v_sql_header varchar2(200):= ' create or replace view '||v_prefix || p_table_name || ' ';
   v_view_col_list varchar2(1000);
   v_sql_text varchar2(200);
   v_convert varchar2(500);
   v_result number;
    
   begin
   
  delete TRADINGANALYSIS.TP_DLAKE_TABLE_VALIDATION where table_name = p_staging_table_name;
   
   -- create view and ensure column order is the same as the staging table
   -- only validate columns that is varchar2 and meant to be strongly typed 
      for rec in (
         with get_columns as (
        select  st.src_table_name, t.table_name,c.column_name field_name,column_id 
        from user_tables t join user_tab_columns c
        on t.table_name=c.table_name
        where t.table_name = p_staging_table_name
        and c.data_type='VARCHAR2'
        order by c.column_id
        )
        select a.src_table_name, a.table_name dest_table_name,a.field_name,b.field_length,b.data_type,b.data_format,b.oracle_type,a.column_id
        from 
        get_columns a  join TRADINGANALYSIS.V_TP_DLAKE_TYPE b
        on UPPER(a.src_table_name) = UPPER(b.table_name) and UPPER(a.field_name) = UPPER(b.field_name) 
        order by dest_table_name,column_id
      )
      loop
      
      
      if REC.ORACLE_TYPE like 'NUMBE%' then
         v_convert:= 'count(TO_NUMBER(TRIM('|| REC.field_name || ')))';
      elsif REC.ORACLE_TYPE like 'DAT%' THEN 
         v_convert:='count(TO_DATE(TRIM('||REC.field_name||'),'||q||rec.data_format||q||'))';
      else
         v_convert:='count(TRIM('|| REC.field_name || '))';
      end if;
      
      BEGIN
      
         v_sql_text:= 'select '||v_convert || ' FROM  TRADINGANALYSIS.' ||REC.DEST_TABLE_NAME 
         ||       ' where rownum<= '||p_check_rows;
         
         
      --   log_message(v_sql_text,'info');
         
         execute immediate v_sql_text into v_result;
         
         exception when others then
         insert into
         tp_dlake_table_validation(TABLE_NAME, FIELD_NAME, DATA_TYPE, FIELD_LENGTH, DATA_FORMAT, ORACLE_TYPE, VALIDATION_ERROR,snapshot_time,DATA_VALUE)
         values(rec.DEST_TABLE_NAME,rec.field_name,
         rec.DATA_TYPE, rec.FIELD_LENGTH, rec.DATA_FORMAT, rec.ORACLE_TYPE, err_stack,sysdate,'SELECT '||REC.FIELD_NAME|| ' FROM '||REC.DEST_TABLE_NAME || ' WHERE ROWNUM<=10;');
         
         
         log_message(rec.field_name || ' ' ||REC.ORACLE_TYPE,'error' );    
         log_message(err_stack,'error');
         commit;
         
              
              

           
   
      end;  
      
      
            
      end loop;
      
    
   end;
   
   procedure validate_tables(p_data_source in varchar2, p_db_link in varchar2, p_check_rows in number default 100)
   is
   begin
   
     
   
     for rec in (select DATA_SOURCE, DB_LINK, OWNER, SRC_TABLE_NAME, STAGING_TABLE_NAME_PREFIX||DEST_TABLE_NAME DEST_TABLE_NAME
      from TRADINGANALYSIS.TP_DLAKE_SNAPSHOT_TABLES where UPPER(active)='Y'  and data_source=p_data_source and db_link = p_db_link)
      loop
         validate_data(rec.dest_table_name, p_check_rows);
      end loop;
       exception when others then 
         log_message(err_stack,'error');   
   
   end;
   
   
END;
/