CREATE OR REPLACE PACKAGE BODY TEMP_DATA.PKG_TP_snapshot_extra
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

   FUNCTION get_tni (p_nmi VARCHAR2)
      RETURN VARCHAR2
   IS
      v_tni   VARCHAR2 (10) := 'UNKNOWN';
   BEGIN
      BEGIN
         SELECT tni
           INTO v_tni
           FROM tp_snap_nmi
          WHERE nmi LIKE SUBSTR (p_nmi, 1, 10);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      RETURN V_TNI;
   END;
   
    PROCEDURE get_netsrepp_data (in_snapshot_id   IN NUMBER,
                                in_start_date    IN DATE,
                                in_finish_date   IN DATE)
   IS
   BEGIN
      FOR rec IN (SELECT table_name
                    FROM user_tables
                   WHERE table_name IN ('TP_SNAP_CALENDAR_HH',
                                        'TP_SNAP_CONTRACTS',
                                        'TP_SNAP_CONTRACT_COMPLETE',
                                        'TP_SNAP_HH_RETAIL',
                                        'TP_SNAP_LOAD_PROFILE',
                                        'TP_SNAP_NMI',
                                        'TP_SNAP_RETAIL_COMPLETE',
                                        'TP_SNAP_RETAIL_SITE',
                                        'TP_SNAP_SITEMETERDATA',
                                        'TP_SNAP_SITE_TABLE'))
      LOOP
         EXECUTE IMMEDIATE 'truncate table  ' || rec.table_name;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'info');


         INSERT INTO tp_snap_calendar_hh
            SELECT *
              FROM retail.snap_calendar_hh@netsrepp.world
             WHERE     snapshot_id = in_snapshot_id
                   AND DATETIME > in_start_date
                   AND datetime <= in_finish_date + 1;


         INSERT INTO TEMP_DATA.TP_SNAP_CONTRACTS
            SELECT *
              FROM retail.SNAP_CONTRACT@netsrepp.world
             WHERE SNAPSHOT_ID = in_snapshot_id;

         INSERT INTO TEMP_DATA.TP_SNAP_CONTRACT_COMPLETE
            SELECT *
              FROM retail.SNAP_CONTRACT_COMPLETE@netsrepp.world
             WHERE SNAPSHOT_ID = in_snapshot_id;

         INSERT INTO TEMP_DATA.TP_SNAP_HH_RETAIL
            SELECT *
              FROM retail.SNAP_HH_RETAIL@netsrepp.world
             WHERE     SNAPSHOT_ID = in_snapshot_id
                   AND DATETIME > in_start_date
                   AND datetime <= in_finish_date + 1;

         INSERT INTO TP_SNAP_load_profile (SNAPSHOT_ID,
                                           SITEID,
                                           PERIOD_CODE,
                                           DAY_CODE,
                                           HH,
                                           VALUE,
                                           CHANGE_DATE,
                                           STD_DEV,
                                           MM_VALUE)
            SELECT in_SNAPSHOT_ID,
                   SITEID,
                   PERIOD_CODE,
                   DAY_CODE,
                   HH,
                   VALUE,
                   CHANGE_DATE,
                   STD_DEV,
                   MM_VALUE
              FROM retail.snap_load_profile@netsrepp.world
             WHERE     snapshot_id = in_snapshot_id
                   AND period_code = TO_CHAR (in_start_date, 'MON');

         INSERT INTO TP_SNAP_nmi (SNAPSHOT_ID,
                                  NMI,
                                  START_DATE,
                                  FINISH_DATE,
                                  TNI,
                                  FRMP,
                                  STATE,
                                  LR,
                                  LNSP,
                                  CLASS_CODE,
                                  METER_INSTALL_CODE,
                                  DLF_CODE,
                                  CHANGED_DATE,
                                  CHANGED_BY)
            SELECT in_SNAPSHOT_ID,
                   NMI,
                   START_DATE,
                   FINISH_DATE,
                   TNI,
                   FRMP,
                   STATE,
                   LR,
                   LNSP,
                   CLASS_CODE,
                   METER_INSTALL_CODE,
                   DLF_CODE,
                   CHANGED_DATE,
                   CHANGED_BY
              FROM retail.snap_nmi@netsrepp.world
             WHERE snapshot_id = in_snapshot_id;

         INSERT INTO TP_SNAP_RETAIL_COMPLETE (SNAPSHOT_ID,
                                              CONTRACT_ID,
                                              REV,
                                              STATE,
                                              LOAD_VARIANCE,
                                              ROLL_IN_ROLL_OUT,
                                              LOADSHEDDING,
                                              EXTENSION,
                                              MEET_THE_MARKET,
                                              DEFAULT_TARIFF_CODE,
                                              AGG_KEY)
            SELECT SNAPSHOT_ID,
                   CONTRACT_ID,
                   REV,
                   STATE,
                   LOAD_VARIANCE,
                   ROLL_IN_ROLL_OUT,
                   LOADSHEDDING,
                   EXTENSION,
                   MEET_THE_MARKET,
                   DEFAULT_TARIFF_CODE,
                   AGG_KEY
              FROM retail.snap_retail_complete@netsrepp.world
             WHERE snapshot_id = in_snapshot_id;

         INSERT INTO tp_snap_retail_site (SNAPSHOT_ID,
                                          CONTRACT_ID,
                                          REV,
                                          SITEID,
                                          START_DATE,
                                          FINISH_DATE,
                                          CONTRACT_VOLUME_ESTIMATE)
            SELECT in_SNAPSHOT_ID,
                   CONTRACT_ID,
                   REV,
                   SITEID,
                   START_DATE,
                   FINISH_DATE,
                   CONTRACT_VOLUME_ESTIMATE
              FROM retail.snap_retail_site@netsrepp.world
             WHERE snapshot_id = in_snapshot_id;

         INSERT INTO tp_snap_sitemeterdata (SNAPSHOT_ID,
                                            DATETIME,
                                            MONTH_NUM,
                                            SITEID,
                                            DEMAND,
                                            CHANGE_DATE,
                                            SETTLED,
                                            STATUS)
            SELECT in_snapshot_id,
                   DATETIME,
                   MONTH_NUM,
                   SITEID,
                   DEMAND,
                   CHANGE_DATE,
                   SETTLED,
                   STATUS
              FROM retail.snap_sitemeterdata@netsrepp.world
             WHERE     snapshot_id = in_snapshot_id
                   AND DATETIME > in_start_date
                   AND datetime <= in_finish_date + 1;

         INSERT INTO TP_SNAP_site_table (SNAPSHOT_ID,
                                         SITEID,
                                         SITE,
                                         STATE,
                                         START_DATE,
                                         FINISH_DATE,
                                         TNI,
                                         MDA,
                                         NMI,
                                         FRMP,
                                         TYPE_FLAG,
                                         METERTYPE,
                                         PAM_SITECODE,
                                         LNSP,
                                         HOLIDAYS,
                                         DLFCODE,
                                         CHANGED_BY,
                                         CHANGED_DATE,
                                         LP,
                                         BUSINESS_UNIT,
                                         DEFAULT_CODE_OVERRIDE,
                                         NMI_STATUS_CODE_ID,
                                         STREAM_FLAG,
                                         DERIVED_DEFAULT_TARIFF_CODE)
            SELECT in_snapshot_id,
                   SITEID,
                   SITE,
                   STATE,
                   START_DATE,
                   FINISH_DATE,
                   TNI,
                   MDA,
                   NMI,
                   FRMP,
                   TYPE_FLAG,
                   METERTYPE,
                   PAM_SITECODE,
                   LNSP,
                   HOLIDAYS,
                   DLFCODE,
                   CHANGED_BY,
                   CHANGED_DATE,
                   LP,
                   BUSINESS_UNIT,
                   DEFAULT_CODE_OVERRIDE,
                   NMI_STATUS_CODE_ID,
                   STREAM_FLAG,
                   DERIVED_DEFAULT_TARIFF_CODE
              FROM retail.snap_site_table@netsrepp.world
             WHERE snapshot_id = in_snapshot_id;

         COMMIT;
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
         v_message := c_s_err || p_message;
      ELSE
         v_message := c_s_info || p_message;
      END IF;

      INSERT INTO message_log (TIME, MESSAGE, USERID)
           VALUES (SYSDATE, v_message, user_name);

      COMMIT;
   END;
   
   PROCEDURE calc_mm_tni (in_snapshot_id   IN NUMBER,
                          in_start_date    IN DATE,
                          in_finish_date   IN DATE)
   IS
   BEGIN
      DELETE TP_HH_TNI_CATEGORY_RESULT
       WHERE snapshot_id = in_snapshot_id AND category_id = g_mm_category;
       
       log_message ('TP_MM_TNI_RESULT', 'info');
       
       INSERT INTO TEMP_DATA.TP_HH_TNI_CATEGORY_RESULT (SNAPSHOT_ID,
                                                       STATE,
                                                       DATETIME,
                                                       CATEGORY_ID,
                                                       TNI,
                                                       MWH
                                                       )       
      SELECT SNAPSHOT_ID,
       STATE,
       DATETIME,
       g_mm_category,
       TNI,
       ( (SETCP_MWH + PPA_MWH) - CI_MWH) MM_MWH
  FROM (SELECT SNAPSHOT_ID,
               STATE,
               DATETIME,
               TNI,
               NVL (SETCP_MWH, 0) SETCP_MWH,
               NVL (CI_MWH, 0) CI_MWH,
               NVL (PPA_MWH, 0) PPA_MWH
          FROM (  SELECT SNAPSHOT_ID,
                         NAME,
                         CASE STATE WHEN 'ACT' THEN 'NSW' ELSE STATE END STATE,
                         DATETIME,
                         TNI,
                         SUM (MWH) MWH
                    FROM V_TP_HH_TNI_CATEGORY_RESULT
                   WHERE     SNAPSHOT_ID = IN_SNAPSHOT_ID
                         AND datetime > in_start_date
                         AND datetime <= in_finish_date +1
                GROUP BY SNAPSHOT_ID,
                         NAME,
                         STATE,
                         DATETIME,
                         TNI) PIVOT (SUM (MWH)
                                 FOR NAME
                                 IN ('SETCP' AS "SETCP_MWH",
                                    'CI' AS "CI_MWH",
                                    'PPA' AS "PPA_MWH")));
         
       
       log_message ('TP_MM_TNI_RESULT  INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         RAISE;
          
   END;
   
   
   PROCEDURE calc_setcp_tni (in_snapshot_id   IN NUMBER,
                          in_start_date    IN DATE,
                          in_finish_date   IN DATE)
   IS
   BEGIN
      DELETE TP_HH_TNI_CATEGORY_RESULT
       WHERE snapshot_id = in_snapshot_id AND category_id = g_setcp_category
       and  datetime > in_start_date
                          AND datetime <= in_finish_date + 1;
       
         INSERT INTO TEMP_DATA.TP_HH_TNI_CATEGORY_RESULT (SNAPSHOT_ID,
                                                       STATE,
                                                       DATETIME,
                                                       MWH,
                                                       CATEGORY_ID,
                                                       TNI)
         select in_snapshot_id,t.state,t.datetime,t.mw/2 mwh,g_setcp_category,tcpid
         from TEMP_DATA.TP_SNAP_SETCPDATA_T t
         where snapshot_id =in_snapshot_id and t.datetime > in_start_date
                          AND t.datetime <= in_finish_date + 1;
      
   END;

    PROCEDURE calc_ci_tni (in_snapshot_id   IN NUMBER,
                          in_start_date    IN DATE,
                          in_finish_date   IN DATE)
   IS
   BEGIN
      DELETE TP_HH_TNI_CATEGORY_RESULT
       WHERE snapshot_id = in_snapshot_id AND category_id = g_ci_category
        and  datetime > in_start_date
                          AND datetime <= in_finish_date + 1;

      INSERT INTO TEMP_DATA.TP_HH_TNI_CATEGORY_RESULT (SNAPSHOT_ID,
                                                       STATE,
                                                       DATETIME,
                                                       CATEGORY_ID,
                                                       TNI,
                                                       MWH)
         WITH get_tni
              AS (  SELECT DISTINCT tni, siteid,start_date,finish_date
                      FROM tp_snap_site_table
                     WHERE snapshot_id = in_snapshot_id
                 )
         select snapshot_id,
                state,
                datetime,
                category,
                tni,
                 sum(qty) qty
                 from (
         SELECT snapshot_id,
                state,
                datetime,
                qty,
                category,
                CASE WHEN tni = 'UNKNOWN' THEN get_tni (nmi) ELSE tni END tni
           FROM (  SELECT snapshot_id,
                          AGG.NMI,
                          agg.STATE,
                          agg.DATETIME,
                          SUM (qty) qty,
                          g_ci_category category,
                          NVL (st.tni, 'UNKNOWN') tni
                     FROM tp_agg_site_hh agg
                          left JOIN get_tni st ON agg.siteid = st.siteid and
                          agg.dt between st.start_date and st.finish_date
                    WHERE     agg.snapshot_id = in_snapshot_id
                          AND agg.datetime > in_start_date
                          AND agg.datetime <= in_finish_date + 1
                 GROUP BY agg.SNAPSHOT_ID,
                          agg.STATE,
                          agg.DATETIME,
                          st.tni,
                          agg.nmi))
                          group by snapshot_id,
                state,
                datetime,
                category,
                tni;

      log_message ('TP_CI_TNI_RESULT  INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         RAISE;
   END;

   PROCEDURE calc_ppa_tni (in_snapshot_id   IN NUMBER,
                           in_start_date    IN DATE,
                           in_finish_date   IN DATE)
   IS
   BEGIN
      DELETE TP_HH_TNI_CATEGORY_RESULT
       WHERE snapshot_id = in_snapshot_id AND category_id = g_ppa_category
        and  datetime > in_start_date
                          AND datetime <= in_finish_date + 1;

      INSERT INTO TEMP_DATA.TP_HH_TNI_CATEGORY_RESULT (SNAPSHOT_ID,
                                                       STATE,
                                                       DATETIME,
                                                       MWH,
                                                       CATEGORY_ID,
                                                       TNI)
           SELECT /*+driving_site (hhr) */ in_snapshot_id,
                  hhr.state,
                  hhr.datetime,
                  SUM (hhr.quantity_mw / 2),
                  g_ppa_category,
                  tni
             FROM Tradinganalysis.V_TP_PPA_HH_TNI@NETS.WORLD hhr
            WHERE     hhr.datetime > in_start_date
                  AND hhr.datetime <= in_finish_date + 1
                  AND hhr.name IN (SELECT c_value
                                     FROM tp_parameters
                                    WHERE name = 'PPA' AND active = 'Y')
         GROUP BY hhr.state, hhr.datetime, hhr.tni;

      log_message ('TP_PPA_TNI_RESULT  INSERT COUNT' || SQL%ROWCOUNT, 'info');
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         RAISE;
   END;
   
     FUNCTION CALCULATE_ACTUAL_PERCENT(in_actual_date in date, in_end_date in date) return number
   is
   v_actual_pc number:=1;
   begin
   
    begin
      SELECT TO_NUMBER(TO_CHAR(in_actual_date,'DD'))/TO_NUMBER(TO_CHAR(in_end_date,'DD')) into v_actual_pc FROM 
      dual;
       log_message ('last actual:'||in_actual_date|| ' end_date:'||in_end_date|| ' actual_percent:'||v_actual_pc, 'info');
      
      return v_actual_pc;
      
      EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         
      end;
      
   end;
   
   
 

   PROCEDURE run (in_snapshot_id   IN NUMBER,
                  in_start_date    IN DATE,
                  in_finish_date   IN DATE)
   IS
   BEGIN
      CALC_CI_TNI (IN_SNAPSHOT_ID, IN_START_DATE, IN_FINISH_DATE);
      CALC_PPA_TNI (IN_SNAPSHOT_ID, IN_START_DATE, IN_FINISH_DATE);
      CALC_SETCP_TNI (IN_SNAPSHOT_ID, IN_START_DATE, IN_FINISH_DATE);
      CALC_MM_TNI (IN_SNAPSHOT_ID, IN_START_DATE, IN_FINISH_DATE);
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
         RAISE;
   END;
END;
/