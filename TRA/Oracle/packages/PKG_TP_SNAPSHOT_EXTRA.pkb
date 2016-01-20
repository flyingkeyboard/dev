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
   
   FUNCTION fix_tni_state_mismatch (p_tni varchar2)
      RETURN VARCHAR2
   IS
      v_tni   VARCHAR2 (10) := 'UNKNOWN';
   BEGIN
   
--   if p_tni = v_tni then 
--      BEGIN
--         SELECT tni
--           INTO v_tni
--           FROM tp_snap_nmi
--          WHERE nmi LIKE SUBSTR (p_nmi, 1, 10);
--      EXCEPTION
--         WHEN NO_DATA_FOUND
--         THEN
--            NULL;
--      END;
--
--      RETURN V_TNI;
null;
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