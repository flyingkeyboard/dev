CREATE OR REPLACE PACKAGE BODY TEMP_DATA.PKG_TP_Snapshot
IS
   /* This package is a cut down version of the NETSREPP transfer pricing snapshot and aggregation process */
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
   
  
   

   FUNCTION Get_Default_Tariff_O (in_site_id        NUMBER,
                                  in_date           DATE,
                                  in_snapshot_id    NUMBER)
      RETURN VARCHAR2
   AS
      v_return   VARCHAR (5);
      v_state    VARCHAR (5);
   BEGIN
      BEGIN
         -- site table
         SELECT s.DEFAULT_CODE_OVERRIDE, s.state
           INTO v_return, v_state
           FROM tp_snap_site_table s
          WHERE     s.SITEid = in_site_id
                AND in_date BETWEEN start_date AND finish_date
                AND snapshot_id = in_snapshot_id;

         IF (LENGTH (v_return) > 0)
         THEN
            RETURN v_return;
         END IF;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            SELECT s.state
              INTO v_state
              FROM tp_snap_site_table s
             WHERE     s.siteid = in_site_id
                   AND snapshot_id = in_snapshot_id
                   AND ROWNUM = 1;
         WHEN OTHERS
         THEN
            RAISE;
      END;

      -- prev contract
      SELECT NVL (
                (SELECT DEFAULT_TARIFF_CODE
                   FROM (SELECT r.DEFAULT_TARIFF_CODE,
                                siteid,
                                ROW_NUMBER ()
                                OVER (
                                   PARTITION BY s.siteid
                                   ORDER BY
                                      LEAST (in_date - c.finish_date) ASC)
                                   rowy
                           FROM tp_snap_contract_complete c,
                                tp_snap_retail_complete r,
                                tp_snap_retail_site s
                          WHERE     c.snapshot_id = in_snapshot_id
                                AND r.snapshot_id = in_snapshot_id
                                AND s.snapshot_id = in_snapshot_id
                                AND c.contract_type = 'RETAIL'
                                AND c.contract_id = r.contract_id
                                AND c.rev = r.rev
                                AND c.built = 1
                                AND c.finish_date <= in_date
                                AND c.contract_id = s.contract_id
                                AND c.rev = s.rev
                                AND s.siteid = in_site_id)
                  WHERE rowy = 1),
                NULL)
        INTO v_return
        FROM DUAL;

      IF (LENGTH (v_return) > 0)
      THEN
         RETURN v_return;
      END IF;

      -- next contract
      SELECT NVL (
                (SELECT DEFAULT_TARIFF_CODE
                   FROM (SELECT r.DEFAULT_TARIFF_CODE,
                                siteid,
                                ROW_NUMBER ()
                                OVER (
                                   PARTITION BY s.siteid
                                   ORDER BY
                                      LEAST (c.start_date - in_date) ASC)
                                   rowy
                           FROM tp_snap_contract_complete c,
                                tp_snap_retail_complete r,
                                tp_snap_retail_site s
                          WHERE     c.snapshot_id = in_snapshot_id
                                AND r.snapshot_id = in_snapshot_id
                                AND s.snapshot_id = in_snapshot_id
                                AND c.contract_type = 'RETAIL'
                                AND c.contract_id = r.contract_id
                                AND c.rev = r.rev
                                AND c.built = 1
                                AND c.start_date >= in_date
                                AND c.contract_id = s.contract_id
                                AND c.rev = s.rev
                                AND s.siteid = in_site_id)
                  WHERE rowy = 1),
                NULL)
        INTO v_return
        FROM DUAL;

      IF (LENGTH (v_return) > 0)
      THEN
         RETURN v_return;
      END IF;

      -- generic
      SELECT NVL (
                (SELECT default_tariff_code
                   FROM tp_snap_cid_tariffs
                  WHERE     default_tariff_type = 'GENERIC'
                        AND superceded = 0
                        AND snapshot_id = in_snapshot_id
                        AND state = v_state
                        AND in_date BETWEEN start_date AND finish_date
                        AND ROWNUM = 1),
                NULL)
        INTO v_return
        FROM DUAL;

      RETURN v_return;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (-20999, in_site_id || '-' || SQLERRM);
         --dbms_output.put_line(substr(sqlerrm,1,200));
         RETURN NULL;
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

   FUNCTION Get_Default_Tariff (in_site_id        NUMBER,
                                in_start_date     DATE,
                                in_finish_date    DATE,
                                in_snapshot_id    NUMBER)
      RETURN VARCHAR2
   AS
      v_return   VARCHAR2 (5);
      v_state    VARCHAR2 (5);
   BEGIN
      BEGIN
         -- site table
         SELECT s.DEFAULT_CODE_OVERRIDE, s.state
           INTO v_return, v_state
           FROM tp_snap_site_table s
          WHERE     s.SITEid = in_site_id
                AND s.start_date <= in_start_date
                AND s.finish_date >= in_finish_date
                AND snapshot_id = in_snapshot_id;

         IF (LENGTH (v_return) > 0)
         THEN
            RETURN v_return;
         END IF;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            SELECT s.state
              INTO v_state
              FROM tp_snap_site_table s
             WHERE     s.siteid = in_site_id
                   AND snapshot_id = in_snapshot_id
                   AND ROWNUM = 1;
         WHEN OTHERS
         THEN
            RAISE;
      END;

      --      -- prev contract
      --      SELECT NVL (
      --                (SELECT DEFAULT_TARIFF_CODE
      --                   FROM (SELECT r.DEFAULT_TARIFF_CODE,
      --                                siteid,
      --                                ROW_NUMBER ()
      --                                OVER (
      --                                   PARTITION BY s.siteid
      --                                   ORDER BY
      --                                      LEAST (in_date - c.finish_date) ASC)
      --                                   rowy
      --                           FROM tp_snap_contract_complete c,
      --                                tp_snap_retail_complete r,
      --                                tp_snap_retail_site s
      --                          WHERE     c.snapshot_id = in_snapshot_id
      --                                AND r.snapshot_id = in_snapshot_id
      --                                AND s.snapshot_id = in_snapshot_id
      --                                AND c.contract_type = 'RETAIL'
      --                                AND c.contract_id = r.contract_id
      --                                AND c.rev = r.rev
      --                                AND c.built = 1
      --                                AND c.finish_date <= in_date
      --                                AND c.contract_id = s.contract_id
      --                                AND c.rev = s.rev
      --                                AND s.siteid = in_site_id)
      --                  WHERE rowy = 1),
      --                NULL)
      --        INTO v_return
      --        FROM DUAL;
      --
      --      IF (LENGTH (v_return) > 0)
      --      THEN
      --         RETURN v_return;
      --      END IF;
      --
      --      -- next contract
      --      SELECT NVL (
      --                (SELECT DEFAULT_TARIFF_CODE
      --                   FROM (SELECT r.DEFAULT_TARIFF_CODE,
      --                                siteid,
      --                                ROW_NUMBER ()
      --                                OVER (
      --                                   PARTITION BY s.siteid
      --                                   ORDER BY
      --                                      LEAST (c.start_date - in_date) ASC)
      --                                   rowy
      --                           FROM tp_snap_contract_complete c,
      --                                tp_snap_retail_complete r,
      --                                tp_snap_retail_site s
      --                          WHERE     c.snapshot_id = in_snapshot_id
      --                                AND r.snapshot_id = in_snapshot_id
      --                                AND s.snapshot_id = in_snapshot_id
      --                                AND c.contract_type = 'RETAIL'
      --                                AND c.contract_id = r.contract_id
      --                                AND c.rev = r.rev
      --                                AND c.built = 1
      --                                AND c.start_date >= in_date
      --                                AND c.contract_id = s.contract_id
      --                                AND c.rev = s.rev
      --                                AND s.siteid = in_site_id)
      --                  WHERE rowy = 1),
      --                NULL)
      --        INTO v_return
      --        FROM DUAL;
      --
      --      IF (LENGTH (v_return) > 0)
      --      THEN
      --         RETURN v_return;
      --      END IF;

      -- generic
      SELECT NVL (
                (SELECT default_tariff_code
                   FROM tp_snap_cid_tariffs
                  WHERE     default_tariff_type = 'GENERIC'
                        AND superceded = 0
                        AND snapshot_id = in_snapshot_id
                        AND state = v_state
                        AND start_date <= in_start_date
                        AND finish_date >= in_finish_date
                        AND ROWNUM = 1),
                NULL)
        INTO v_return
        FROM DUAL;

      RETURN v_return;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (-20999, in_site_id || '-' || SQLERRM);
         --dbms_output.put_line(substr(sqlerrm,1,200));
         RETURN NULL;
   END;


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

   PROCEDURE index_maintain (in_action VARCHAR2 DEFAULT NULL)
   IS
   BEGIN
      IF in_action = 'CREATE'
      THEN
         EXECUTE IMMEDIATE
            'CREATE INDEX TP_AGG_SITE_HH_IDX1 ON TP_AGG_SITE_HH(SNAPSHOT_ID,  SITEID, DATETIME)';
      ELSIF in_action = 'DROP'
      THEN
         EXECUTE IMMEDIATE 'DROP INDEX TP_AGG_SITE_HH_IDX1';
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'info');
   END;



   PROCEDURE reload_agg_site_hh (in_section VARCHAR2)
   IS
      v_rowcount   NUMBER;
   BEGIN
      SELECT COUNT (*) INTO v_rowcount FROM tp_agg_site_hh_temp;

      IF v_rowcount > 0
      THEN
         log_message ('wipe tp_agg_site_hh ', 'info');

        
         EXECUTE IMMEDIATE 'drop table tp_agg_site_hh';
        
         
         execute immediate 'create table tp_agg_site_hh as select * from tp_agg_site_hh_1 where 1=0';

         INSERT INTO tp_agg_site_hh (SNAPSHOT_ID,
                                     SITEID,
                                     CONTRACT_ID,
                                     REV,
                                     NMI,
                                     STATE,
                                     DATETIME,
                                     DT,
                                     MONTH_NUM,
                                     PEAK_FLAG,
                                     QTY,
                                     DEMAND,
                                     LP_STATUS)
            SELECT SNAPSHOT_ID,
                   SITEID,
                   CONTRACT_ID,
                   REV,
                   NMI,
                   STATE,
                   DATETIME,
                   DT,
                   MONTH_NUM,
                   PEAK_FLAG,
                   QTY,
                   DEMAND,
                   LP_STATUS
              FROM tp_agg_site_hh_temp;

         log_message (
               'tp_agg_site_hh applied '
            || in_section
            || ' COUNT'
            || SQL%ROWCOUNT,
            'info');
      ELSE
         log_message (
               'tp_agg_site_hh_temp is empty, something has gone wrong: '
            || in_section,
            'info');
      END IF;
   END;


   PROCEDURE Do_Snapshot (in_snapshot_id           NUMBER,
                          in_start_date         IN DATE,
                          in_finish_date        IN DATE,
                          in_gather_stats       IN VARCHAR2 DEFAULT 'Y',
                          in_snap_contract      IN VARCHAR2 DEFAULT 'Y',
                          in_snap_ref_tables    IN VARCHAR2 DEFAULT 'Y',
                          in_snap_hh_retail     IN VARCHAR2 DEFAULT 'Y',
                          in_snap_retail_site   IN VARCHAR2 DEFAULT 'Y',
                          in_snap_smeter        IN VARCHAR2 DEFAULT 'N',
                          in_snap_lp            IN VARCHAR2 DEFAULT 'N',
                          in_agg_cid            IN VARCHAR2 DEFAULT 'Y',
                          in_agg_site_hh        IN VARCHAR2 DEFAULT 'Y',
                          in_apply_meterdata    IN VARCHAR2 DEFAULT 'Y',
                          in_apply_lp           IN VARCHAR2 DEFAULT 'Y',
                          in_cleanup_act        IN VARCHAR2 DEFAULT 'Y')
   IS
   out_load_request       SYS_REFCURSOR;
   BEGIN
      
    
      
      IF in_snap_hh_retail = 'Y'
      THEN
         log_message ('start retail snapshot '||in_start_date || in_finish_date, 'info');
         snapshot_hh_retail (in_snapshot_id, in_start_date, in_finish_date);
      END IF;

      IF in_snap_smeter = 'Y'
      THEN
         log_message ('start sitemeterdata snapshot '||in_start_date || in_finish_date, 'info');
         snapshot_sitemeterdata (in_snapshot_id,
                                 in_start_date,
                                 in_finish_date);
      END IF;

      IF in_snap_contract = 'Y'
      THEN
         log_message ('start contracts '||in_start_date || in_finish_date, 'info');
         snapshot_contracts (in_snapshot_id, in_start_date, in_finish_date);
      END IF;

      IF in_snap_ref_tables = 'Y'
      THEN
         log_message ('start ref tables snapshot '||in_start_date || in_finish_date, 'info');
         snapshot_ref_tables (in_snapshot_id, in_start_date, in_finish_date);
         log_message ('start setcp,ppa tables snapshot '||in_start_date || in_finish_date, 'info');
         snapshot_setcpdata (in_snapshot_id, in_start_date, in_finish_date);
      END IF;

      IF in_snap_retail_site = 'Y'
      THEN
         snapshot_retail_site (in_snapshot_id, in_start_date, in_finish_date);
      END IF;



      IF in_snap_lp = 'Y'
      THEN
         log_message ('start load profile tables snapshot '||in_start_date || in_finish_date, 'info');
         snapshot_load_profile (in_snapshot_id,
                                in_start_date,
                                in_finish_date);
      END IF;

      index_maintain ('DROP');

      IF in_gather_stats = 'Y'
      THEN
         gather_stats;
      END IF;


      EXECUTE IMMEDIATE 'truncate table tP_agg_site_hh';

      log_message ('wipe tp_agg_site_hh_temp ', 'info');

      --  END IF;

      IF in_agg_cid = 'Y'
      THEN
         Create_CID_Records (in_snapshot_id, in_start_date, in_finish_date);
      END IF;


    

      IF in_agg_site_hh = 'Y'
      THEN
         load_agg_site_hh (in_snapshot_id,
                           in_start_date,
                           in_finish_date,
                           NULL);
         backfill_agg_site_hh (in_snapshot_id, in_start_date, in_finish_date);
         gather_stats('TP_AGG_SITE_HH');
      END IF;

      IF in_apply_meterdata = 'Y'
      THEN
         apply_meterdata (in_snapshot_id, in_start_date, in_finish_date);
      END IF;

  index_maintain ('CREATE');

      IF in_apply_lp = 'Y'
      THEN
         apply_load_profile (in_snapshot_id, in_start_date, in_finish_date);
      END IF;



      IF in_cleanup_act = 'Y'
      THEN
         cleanup_act;
      END IF;

      agg_mass_market (in_snapshot_id, in_start_date, in_finish_date);
      -- only apply adjustment on current month
      additional_adjustment (in_snapshot_id, trunc(in_finish_date,'MON'), in_finish_date);
      -- create index on tp_agg_site_hh to speed up query performance
    
      
      refresh_views(in_snapshot_id, in_start_date, in_finish_date);
      
      -- TNI level aggregation
      PKG_TP_SNAPSHOT_EXTRA.RUN(in_snapshot_id, in_start_date, in_finish_date);
   
      commit;
   END;



   PROCEDURE snapshot_contracts (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE)
   IS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table TP_SNAP_CONTRACTS';

      INSERT INTO TP_SNAP_CONTRACTs (SNAPSHOT_ID,
                                     CONTRACT_ID,
                                     REV,
                                     CONTRACT_TYPE,
                                     CONTRACT_NAME,
                                     CONTRACT_GROUP,
                                     PORTFOLIO,
                                     START_DATE,
                                     FINISH_DATE,
                                     CP_ID,
                                     DEAL_NUMBER,
                                     TRADING_GROUP,
                                     TRADING_PROGRAM,
                                     TRADE_DATE,
                                     CHANGED_BY,
                                     CHANGED_DATE,
                                     AUTHORISED,
                                     AUTHORISED_BY,
                                     VERIFIED,
                                     VERIFIED_BY,
                                     CONFIRMED,
                                     CONFIRMED_BY,
                                     BUSINESS_UNIT,
                                     STATE,
                                     SUPERCEDED,
                                     BUILT,
                                     BUILD_CHANGE)
         SELECT in_snapshot_id,
                CONTRACT_ID,
                REV,
                CONTRACT_TYPE,
                CONTRACT_NAME,
                CONTRACT_GROUP,
                PORTFOLIO,
                START_DATE,
                FINISH_DATE,
                NVL (CP_ID, 242),
                DEAL_NUMBER,
                TRADING_GROUP,
                TRADING_PROGRAM,
                TRADE_DATE,
                CHANGED_BY,
                CHANGED_DATE,
                AUTHORISED,
                AUTHORISED_BY,
                VERIFIED,
                VERIFIED_BY,
                CONFIRMED,
                CONFIRMED_BY,
                BUSINESS_UNIT,
                STATE,
                SUPERCEDED,
                BUILT,
                BUILD_CHANGE
           FROM v_tp_src_contract c;

      log_message ('TP_SNAP_CONTRACT INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;


   PROCEDURE snapshot_hh_retail (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE)
   IS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table tp_SNAP_HH_RETAIL';

      INSERT INTO tp_SNAP_HH_RETAIL (SNAPSHOT_ID,
                                     MONTH_NUM,
                                     CONTRACT_ID,
                                     REV,
                                     STATE,
                                     DATETIME,
                                     DAY,
                                     HH,
                                     QUANTITY,
                                     FIXED_PRICE,
                                     PRICE_ESCALATOR,
                                     FORECAST_FLAG)
         SELECT in_snapshot_id,
                TO_CHAR (datetime, 'MM'),
                contract_id,
                rev,
                state,
                datetime,
                day,
                hh,
                quantity_s,
                fixed_price * NVL (price_escalator, 1) escalated_price,
                price_escalator,
                status
           FROM tp_hh_retail
          WHERE      --datetime >= ADD_MONTHS (TRUNC (SYSDATE, 'MM'), -18) AND
               day   BETWEEN IN_START_date AND in_finish_date
                AND quantity_s IS NOT NULL
                AND status IS NOT NULL;

      log_message ('tp_SNAP_HH_RETAIL INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;


   PROCEDURE snapshot_retail_site (in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE)
   IS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table TP_SNAP_RETAIL_SITE';

      INSERT INTO TP_SNAP_RETAIL_SITE
         SELECT in_snapshot_id, s.*
           FROM TP_RETAIL_SITE s;

      log_message ('TP_SNAP_RETAIL_SITE INSERT COUNT' || SQL%ROWCOUNT,
                   'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;

   PROCEDURE snapshot_load_profile (in_snapshot_id   IN NUMBER,
                                    in_start_date    IN DATE,
                                    in_finish_date   IN DATE)
   IS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table tp_snap_load_profile';


      INSERT INTO tp_SNAP_LOAD_PROFILE (SNAPSHOT_ID,
                                        SITEID,
                                        PERIOD_CODE,
                                        DAY_CODE,
                                        HH,
                                        VALUE,
                                        CHANGE_DATE,
                                        STD_DEV,
                                        MM_VALUE)
         SELECT in_snapshot_id,
                SITEID,
                PERIOD_CODE,
                DAY_CODE,
                HH,
                VALUE,
                CHANGE_DATE,
                STD_DEV,
                MM_VALUE
           FROM tp_load_profile s
          WHERE PERIOD_CODE IN (    SELECT TO_CHAR (
                                              ADD_MONTHS (start_date, LEVEL - 1),
                                              'MON')
                                      FROM (SELECT in_start_date start_date,
                                                   in_finish_date + 1 end_date
                                              FROM DUAL)
                                CONNECT BY LEVEL <=
                                                MONTHS_BETWEEN (
                                                   TRUNC (end_date, 'MM'),
                                                   TRUNC (start_date, 'MM'))
                                              * +1);



      log_message ('tp_SNAP_LOAD_PROFILE  INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;

   PROCEDURE snapshot_ref_tables (in_snapshot_id   IN NUMBER,
                                  in_start_date    IN DATE,
                                  in_finish_date   IN DATE)
   IS
   BEGIN
      BEGIN
         FOR rec IN (SELECT table_name
                       FROM user_tables
                      WHERE table_name IN ('TP_SNAP_CONTRACT_COMPLETE',
                                           'TP_SNAP_SITE_TABLE',
                                           'TP_SNAP_DLF_CODES',
                                           'TP_SNAP_MLF_CODES',
                                           'TP_SNAP_COUNTERPARTY',
                                           'TP_SNAP_CID_TARIFFS',
                                           'TP_SNAP_CID_PRICES',
                                           'TP_SNAP_REGIONAL_PRICES',
                                           'TP_SNAP_COMPANY_IDENTIFIER',
                                           'TP_SNAP_NMI',
                                           'TP_SNAP_RETAIL_COMPLETE',
                                           'TP_SNAP_CALENDAR_HH',
                                           'TP_SNAP_HOLIDAY'))
         LOOP
            EXECUTE IMMEDIATE 'truncate table  ' || rec.table_name;
         END LOOP;
      EXCEPTION
         WHEN OTHERS
         THEN
            log_message (err_stack, 'info');
      END;

      --      EXECUTE IMMEDIATE 'truncate table tp_SNAP_CALENDAR_HH';
      --
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_contract_complete';
      --
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_site_table';
      --
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_dlf_codes';
      --
      --      EXECUTE IMMEDIATE 'truncate table tp_SNAP_mlf_codes';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_COUNTERPARTY';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_CID_TARIFFS';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_CID_PRICES';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_REGIONAL_PRICES';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_COMPANY_IDENTIFIER';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_NMI';
      --
      --      execute Immediate 'truncate table TP_snap_retail_complete';
      --
      --      execute immediate 'truncate table tp_snap_holiday';


      INSERT INTO TP_SNAP_HOLIDAY (SNAPSHOT_ID,
                                   HOLIDAY_NAME,
                                   HOLIDAY_DATE,
                                   USERID,
                                   CHANGE_DATE)
         SELECT in_snapshot_id,
                HOLIDAY_NAME,
                HOLIDAY_DATE,
                USERID,
                CHANGE_DATE
           FROM TP_HOLIDAY;


      INSERT                                                      /* APPEND */
            INTO  TP_SNAP_NMI
         SELECT in_snapshot_id,
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
           FROM TP_v_src_nmi;

      log_message ('TP_SNAP_NMI INSERT COUNT' || SQL%ROWCOUNT, 'info');


      INSERT                                                      /* APPEND */
            INTO  tp_SNAP_COMPANY_IDENTIFIER
         SELECT in_snapshot_id,
                COMPANY_CODE,
                FRMP,
                LNSP,
                LR,
                OE_START_DATE,
                OE_FINISH_DATE
           FROM tp_COMPANY_IDENTIFIER s;

      log_message ('tp_SNAP_COMPANY_IDENTIFIER INSERT COUNT' || SQL%ROWCOUNT,
                   'info');


      INSERT                                                      /* APPEND */
            INTO  TP_SNAP_CID_TARIFFS
         SELECT in_snapshot_id,
                DEFAULT_TARIFF_CODE,
                REV,
                REFERENCE,
                CHANGED_BY,
                CHANGED_DATE,
                VERIFIED,
                VERIFIED_BY,
                VERIFIED_DATE,
                SUPERCEDED,
                BUILT,
                STATE,
                START_DATE,
                FINISH_DATE,
                TERM_CHANGED_BY,
                TERM_CHANGED_DATE,
                CALENDAR_ID,
                CHARGE_TYPE,
                VALUE,
                INCLUDED_IN_TRANSFER_PRICE,
                INCLUDED_IN_BILLING_PRICE,
                DEFAULT_TARIFF_TYPE
           FROM tp_v_src_cid_tariffs;

      log_message ('TP_SNAP_CID_TARIFFS INSERT COUNT' || SQL%ROWCOUNT,
                   'info');



      INSERT                                                      /* APPEND */
            INTO  TP_SNAP_CID_PRICES
         SELECT in_snapshot_id,
                CASE
                   WHEN charge_type = 'RRP' THEN rrp
                   WHEN charge_type = 'FACTOR' THEN rrp * (VALUE / 100)
                   WHEN charge_type = 'ADMIN' THEN VALUE
                   WHEN charge_type = 'FIXED' THEN VALUE
                   ELSE -1
                END
                   CID_PRICE,
                a.state,
                a.DEFAULT_TARIFF_CODE,
                b.DATETIME,
                calendar_id
           FROM tp_v_cid_tariffs a,
                tp_regional_prices b,
                TEMP_DATA.TP_CALENDAR_HH c
          WHERE     a.state = b.state
                AND b.datetime BETWEEN a.start_date AND a.finish_date
                AND b.datetime >= in_start_date
                AND b.datetime <= in_finish_date + 1
                AND a.superceded = 0
                AND included_in_transfer_price = 'YES'
                AND c.market_datetime = b.DATETIME
                AND c.ID = a.CALENDAR_ID;

      log_message ('TP_SNAP_CID_PRICES INSERT COUNT' || SQL%ROWCOUNT, 'info');


      INSERT                                                      /* APPEND */
            INTO  tp_snap_regional_prices
         SELECT in_snapshot_id,
                STATE,
                DATETIME,
                RRP,
                TO_CHAR (DATETIME - 1 / 48, 'MM'),
                STATUS,
                CHANGE_DATE
           FROM tp_regional_prices
          WHERE datetime > SYSDATE - 400;


      log_message ('tp_snap_regional_prices INSERT COUNT' || SQL%ROWCOUNT,
                   'info');



      INSERT INTO TP_SNAP_COUNTERPARTY (SNAPSHOT_ID,
                                        CP_ID,
                                        CP_NAME,
                                        CP_FLAG,
                                        CP_INSTITUTION_TYPE)
         SELECT in_snapshot_id,
                CP_ID,
                CP_NAME,
                CP_FLAG,
                CP_INSTITUTION_TYPE
           FROM v_tp_src_counterparty c;

      log_message ('TP_SNAP_COUNTERPARTY INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      INSERT INTO tp_SNAP_CALENDAR_HH
         SELECT in_snapshot_id,
                DATETIME,
                DT,
                HH,
                STATE,
                PERIOD_CODE,
                PEAK_FLAG
           FROM v_tp_src_calendar_hh
          WHERE dt BETWEEN IN_START_date AND in_finish_date;

      log_message ('tp_SNAP_CALENDAR_HH INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      COMMIT;

      INSERT INTO tp_snap_site_table
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
                NVL (holidays, 'NONE') HOLIDAYS,
                DLFCODE,
                CHANGED_BY,
                CHANGED_DATE,
                LP,
                BUSINESS_UNIT,
                DEFAULT_CODE_OVERRIDE,
                NMI_STATUS_CODE_ID,
                STREAM_FLAG,
                NULL
           FROM V_TP_SRC_SITE_TABLE s;

      log_message ('tp_snap_site_table INSERT COUNT' || SQL%ROWCOUNT, 'info');

      COMMIT;


      INSERT INTO tp_snap_contract_complete
         SELECT in_snapshot_id,
                con.contract_id,
                con.rev,
                con.contract_type,
                con.contract_name,
                con.contract_group,
                CASE con.portfolio
                   WHEN 'RETAIL' THEN 'POWERCOR'
                   ELSE con.portfolio
                END
                   portfolio,
                con.start_date,
                con.finish_date,
                con.counterparty cp_id,
                con.deal_number,
                con.trading_group,
                con.trading_program,
                con.trade_date,
                con.changed_by,
                con.changed_date,
                con.authorised,
                con.authorised_by,
                con.verified,
                con.verified_by,
                con.confirmed,
                con.confirmed_by,
                CASE cp.institution_type
                   WHEN 'ERM CUSTOMER'
                   THEN
                      'ERM CUSTOMER'
                   ELSE
                      SUBSTR (con.contract_group,
                              1,
                              INSTR (con.contract_group, '-') - 1)
                END
                   business_unit,
                SUBSTR (con.contract_group,
                        INSTR (con.contract_group, '-') + 1)
                   state,
                con.superceded,
                con.built,
                con.build_change,
                con.status,
                con.verified_date,
                con.lastupdate,
                CASE con.contract_type
                   WHEN 'RETAIL' THEN aky.DESCRIPTION
                   ELSE con.contract_type
                END
                   description
           FROM tp_contracts con,
                tp_retails ret,
                tp_retail_agg_key aky,
                tp_counterparty cp
          WHERE     con.contract_id = ret.contract_id(+)
                AND con.rev = ret.rev(+)
                AND ret.agg_key = aky.AGG_KEY(+)
                AND con.counterparty = cp.counterparty_id(+);

      log_message ('tp_snap_contract_complete INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      COMMIT;

      INSERT INTO tp_snap_dlf_codes
         SELECT in_snapshot_id,
                DLFCODE,
                JURISDICTIONCODE,
                STARTDATE,
                DLFDESC,
                DLFVALUE,
                ENDDATE,
                UPDATED,
                CHANGED_BY,
                CHANGED_DATE
           FROM tp_dlf;

      log_message ('tp_snap_dlf_codes INSERT COUNT' || SQL%ROWCOUNT, 'info');

      COMMIT;

      INSERT INTO tp_snap_mlf_codes
         SELECT in_snapshot_id,
                TNICODE,
                REGIONID,
                STARTDATE,
                ENDDATE,
                MLF,
                ID,
                UPDATED,
                CHANGED_BY,
                CHANGED_DATE
           FROM tp_mlf;

      log_message ('tp_snap_mlf_codes INSERT COUNT' || SQL%ROWCOUNT, 'info');



      INSERT INTO TP_snap_retail_complete
         SELECT in_snapshot_id,
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
           FROM tp_retails;

      log_message ('tp_snap_retail_complete INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         RAISE;
   END;



   PROCEDURE snapshot_sitemeterdata (in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE,
                                     in_siteid        IN NUMBER)
   IS
   BEGIN
      EXECUTE IMMEDIATE 'truncate table tp_SNAP_SITEMETERDATA';

      INSERT INTO tp_SNAP_SITEMETERDATA
         SELECT in_snapshot_id,
                DATETIME,
                TO_CHAR (datetime, 'mm'),
                SITEID,
                DEMAND,
                CHANGE_DATE,
                SETTLED,
                STATUS
           FROM tp_sitemeterdata
          WHERE                  --(siteid=in_siteid or in_siteid is null) and
                datetime > IN_START_date AND datetime <= in_finish_date + 1;

      log_message ('tp_SNAP_SITEMETERDATA INSERT COUNT' || SQL%ROWCOUNT,
                   'info');
      COMMIT;
   END;



   PROCEDURE snapshot_sitemeterdata (in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE)
   IS
   BEGIN
      snapshot_sitemeterdata (in_snapshot_id,
                              in_start_date,
                              in_finish_date,
                              NULL);
   END;


   PROCEDURE snapshot_setcpdata (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE)
   IS
      v_actual_date  date;
      v_max_date   date;
      v_actual_pc         NUMBER:=1;
   BEGIN
      EXECUTE IMMEDIATE 'truncate table tp_snap_ppa_hh';

      EXECUTE IMMEDIATE 'truncate table tp_snap_setcpdata_t';

      INSERT INTO tp_snap_setcpdata_t (snapshot_id,tcpid,
                                       datetime,
                                       state,
                                       participantid,
                                       mw,
                                       NO_TLF_mw
                                       )
           SELECT in_snapshot_id,tcpid,
                  datetime,
                  state,
                  participantid,
                  SUM (mwh) * 2 AS mw,
                  SUM (NO_TLF_mwh) * 2 AS NO_TLF_mw
             FROM (  SELECT cp.settlementdate + cp.periodid / 48 AS datetime,
                            RTRIM (cp.regionid, 1) AS state,
                            participantid,
                            tcpid,
                            SUM (-1 * cp.ta * cp.tlf) AS mwh,
                            SUM (-1 * cp.ta) AS NO_TLF_mwh
                       FROM mms.setcpdata@sgpss.world cp,
                            (  SELECT bd.settlementdate, MAX (bd.runno) max_runno
                                 FROM mms.billingdaytrk@sgpss.world bd
                                WHERE     settlementdate >= in_start_date
                                      AND settlementdate <= in_finish_date
                                      AND bd.runno <> 1 -- temp fix to get around data issue with D+1
                             GROUP BY bd.settlementdate) vers
                      WHERE     cp.settlementdate = vers.settlementdate
                            AND cp.versionno = vers.max_runno
                   GROUP BY cp.settlementdate + cp.periodid / 48,
                            participantid,
                            cp.regionid,
                            cp.tcpid)
         GROUP BY datetime,
                  state,
                  participantid,
                  tcpid;


      log_message ('tp_snap_setcpdata_t INSERT COUNT' || SQL%ROWCOUNT,
                   'info');

      
     
      
      SELECT trunc(MAX(DATETIME-1/48)) into v_actual_date  FROM TP_SNAP_SETCPDATA_T;
      v_actual_pc:=calculate_actual_percent(v_actual_date,in_finish_date); 
        log_message ('tp_snap_setcpdata_t actual percentage:' || to_char(v_actual_pc,'999.999'),
                   'info');
        
  
-- only apply extrapolation to current month
      UPDATE tp_snap_setcpdata_t
         SET mw_inc_forecast = mw / v_actual_pc,
             mw_adjustment = (mw / v_actual_pc) - mw
             where datetime > trunc(in_finish_date,'MON')
             ;


      INSERT INTO tp_snap_ppa_hh (snapshot_id,
                                 name,
                                  state,
                                  datetime,
                                  quantity_mw)
         select in_snapshot_id,name,
                state,
                datetime,
                quantity_mw from (
         SELECT name,
                state,
                datetime,
                quantity_mw
           FROM tradinganalysis.mv_ppa_hh@nets.world
          WHERE datetime > in_start_date AND datetime <= in_finish_date + 1
          and name in (select c_value from tp_parameters where name = 'PPA' and active='Y')
         UNION
         SELECT name,
                state,
                datetime,
                quantity_mw
           FROM tradinganalysis.MV_GFS_PPA_HH_ACTUAL@nets.world
          WHERE datetime > in_start_date AND datetime <= in_finish_date + 1
          and name in (select c_value from tp_parameters where name = 'PPA' and active='Y')
          )
          
--            INSERT INTO tp_snap_ppa_hh (snapshot_id,
--                                 name,
--                                  state,
--                                  datetime,
--                                  quantity_mw)
--         select in_snapshot_id,name,
--                state,
--                datetime,
--                quantity_mw from (
--         SELECT name,
--                state,
--                datetime,
--                quantity_mw
--           FROM tradinganalysis.MV_GFS_PPA_HH_TNI_ACTUAL@nets.world
--          WHERE datetime > in_start_date AND datetime <= in_finish_date + 1
--          --and name in (select c_value from tp_parameters where name = 'PPA' and active='Y')
--          )
--          
          ;

      log_message ('tp_snap_ppa_hh INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
         RAISE;
   END;


   PROCEDURE load_agg_site_hh (in_snapshot_id   IN NUMBER,
                               in_start_date    IN DATE,
                               in_finish_date   IN DATE,
                               in_siteid        IN NUMBER)
   IS
   BEGIN
      INSERT INTO tp_agg_site_hh (snapshot_id,
                                  siteid,
                                  contract_id,
                                  rev,
                                  state,
                                  nmi,
                                  datetime,
                                  dt,
                                  month_num,
                                  peak_flag,
                                  qty,
                                  DEMAND,
                                  lp_status)
         SELECT                                          /* PARALLEL(HHR,4) */
               con.snapshot_id snapshot_id,
                hhs.siteid,
                con.contract_id,
                con.rev,
                hhr.state,
                sta.nmi,
                hhr.datetime,
                TRUNC (hhr.datetime - 1 / 48) dt,
                TO_CHAR ( (hhr.datetime - 1 / 48), 'MM') month_num,
                cal.peak_flag,
                0 qty,
                0 demand,
                0 lp_status
           FROM tp_snap_contracts con,
                tp_snap_hh_retail hhr,
                tp_snap_retail_site hhs,
                tp_snap_calendar_hh cal,
                tp_snap_site_table sta
          WHERE     con.snapshot_id = in_snapshot_id
                AND sta.siteid = hhs.siteid
                AND con.contract_id = hhr.contract_id
                AND con.REV = hhs.REV
                AND hhr.snapshot_id = con.snapshot_id
                AND hhr.contract_id = hhs.contract_id
                AND hhs.snapshot_id = con.snapshot_id
                AND hhr.DAY BETWEEN hhs.start_date AND hhs.finish_date
                AND cal.datetime = hhr.datetime
                AND cal.snapshot_id = con.snapshot_id
                AND cal.state = hhr.state
                AND sta.snapshot_id = con.snapshot_id
                AND hhr.DAY BETWEEN sta.start_date AND sta.finish_date
                AND hhr.day BETWEEN IN_START_date AND in_finish_date
                AND sta.siteid = hhs.siteid
                AND sta.TYPE_FLAG = 1                                       --
                                     ;

      log_message ('tp_agg_site_hh INSERT COUNT' || SQL%ROWCOUNT, 'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
         RAISE;                                       -- no need to go further
   END;


   PROCEDURE backfill_agg_site_hh (in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE)
   IS
   BEGIN
      INSERT INTO tp_agg_site_hh (snapshot_id,
                                  siteid,
                                  contract_id,
                                  rev,
                                  state,
                                  nmi,
                                  datetime,
                                  dt,
                                  month_num,
                                  peak_flag,
                                  qty,
                                  DEMAND,
                                  lp_status)
         WITH recs_there
              AS (SELECT datetime, siteid
                    FROM tp_agg_site_hh a, tp_snap_contract_complete b
                   WHERE     a.contract_id = b.contract_id
                         AND a.rev = b.rev
                         AND b.snapshot_id = in_snapshot_id
                         AND b.contract_type = 'RETAIL'),
              backfill
              AS (SELECT in_snapshot_id snapshot_id,
                         ret.siteid,
                         ret.CONTRACT_ID,
                         ret.rev,
                         sit.nmi,
                         sit.STATE,
                         cal.DATETIME,
                         cal.dt,
                         TO_CHAR (cal.dt, 'MM') month_num,
                         cal.PEAK_FLAG,
                         0 qty,
                         0 demand,
                         0 lp_status
                    FROM tp_snap_retail_site ret,
                         tp_snap_calendar_hh cal,
                         tp_snap_site_table sit,
                         tp_snap_contracts con
                   WHERE     ret.snapshot_id = in_snapshot_id
                         AND sit.snapshot_id = ret.snapshot_id
                         AND cal.snapshot_id = ret.snapshot_id
                         AND con.snapshot_id = ret.snapshot_id
                         AND ret.SITEID = sit.SITEID
                         AND con.contract_id = ret.contract_id
                         AND con.rev = ret.rev
                         AND cal.dt BETWEEN sit.START_DATE
                                        AND sit.FINISH_DATE
                         AND cal.dt BETWEEN ret.START_DATE
                                        AND ret.FINISH_DATE
                         AND cal.DT BETWEEN IN_START_date AND in_finish_date
                         AND cal.state = sit.STATE
                         AND sit.TYPE_FLAG = 1 --   AND CON.CONTRACT_ID IN (SELECT CONTRACT_ID FROM TP_CONTRACT_FILTER WHERE ACTIVE_FLAG='Y')
                                              )
         SELECT a.snapshot_id,
                a.siteid,
                a.CONTRACT_ID,
                a.rev,
                a.STATE,
                a.nmi,
                a.DATETIME,
                a.dt,
                a.month_num,
                a.PEAK_FLAG,
                a.qty,
                a.demand,
                a.lp_status
           FROM backfill a, recs_there
          WHERE     recs_there.SITEID(+) = a.SITEID
                AND recs_there.DATETIME(+) = a.datetime
                AND recs_there.DATETIME IS NULL;

      log_message ('tp_agg_site_hh backfill INSERT COUNT' || SQL%ROWCOUNT,
                   'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;

   PROCEDURE Create_CID_Records_O (in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE)
   IS
   /*** This the obsolete CREATE_CID procedure which is very slow ***/
      v_affected_count     NUMBER;
      v_this_day           DATE;
      v_snapshot_type      VARCHAR2 (100);

      v_site_ids           number_list;
      v_default_codes      varchar2_list;
      v_last_month_count   NUMBER;
   BEGIN
      v_this_day := in_start_date;

      EXECUTE IMMEDIATE 'delete  tp_cid_code_temp1';

      -- TODO - look at removing loop - test server is pretty poor so required for this environment
      WHILE (v_this_day <= in_finish_date)
      LOOP
         -- quicker to calc all this in one go and post to temporary table
         -- significantly faster + we get a lot of latch locks otherwise

         INSERT INTO tp_cid_code_temp
            SELECT siteid,
                   Get_Default_Tariff_O (siteid, v_this_day, in_snapshot_id)
                      day_tariff_code
              FROM tp_snap_site_table
             WHERE     v_this_day BETWEEN start_date AND finish_date
                   AND snapshot_id = in_snapshot_id;

         -- now insert what should be populated
         INSERT INTO tp_agg_site_hh (SNAPSHOT_ID,
                                     SITEID,
                                     CONTRACT_ID,
                                     REV,
                                     NMI,
                                     STATE,
                                     DATETIME,
                                     DT,
                                     MONTH_NUM,
                                     PEAK_FLAG,
                                     QTY,
                                     DEMAND,
                                     LP_STATUS,
                                     PRICE)
            WITH required_dates
                 AS (SELECT wva.start_date, wva.finish_date, siteid
                       FROM tp_snap_retail_site wva,
                            tp_snap_contract_complete con
                      WHERE     con.snapshot_id = in_snapshot_id
                            AND wva.snapshot_id = in_snapshot_id
                            AND built = 1
                            AND v_this_day BETWEEN wva.start_date
                                               AND wva.finish_date
                            AND con.contract_id = wva.contract_id
                            AND con.rev = wva.rev)
            SELECT                      /* USE_HASH(nmi sit cal cid_prices) */
                  in_snapshot_id,
                   sit.siteid,
                   con.contract_id,
                   con.rev,
                   nmi.nmi,
                   sit.state,
                   cal.datetime,
                   dt,
                   TO_CHAR ( (cal.datetime - 1 / 48), 'MM') month_num,
                   cal.peak_flag,
                   0,
                   0,
                   0,
                   cid_prices.cid_price
              FROM tp_snap_nmi nmi,
                   tp_snap_site_table sit,
                   tp_snap_calendar_hh cal,
                   tp_snap_cid_prices cid_prices,
                   tp_snap_contracts con,
                   required_dates,
                   tp_cid_code_temp ctp
             WHERE     nmi.SNAPSHOT_ID = in_snapshot_id
                   AND sit.snapshot_id = in_snapshot_id
                   AND cal.snapshot_id = in_snapshot_id
                   AND con.snapshot_id = in_snapshot_id
                   AND cal.dt = v_this_day --between trunc(in_month,'MON') and last_day(in_month)
                   --and     cal.dt = cal.datetime - 1 -- remove this for 48 hh records
                   AND cid_prices.state = sit.state
                   AND cid_prices.datetime = cal.datetime
                   AND cid_prices.snapshot_id = in_snapshot_id
                   AND sit.nmi = nmi.nmi
                   AND cal.dt BETWEEN sit.start_date AND sit.finish_date
                   AND cal.dt BETWEEN nmi.start_date AND nmi.finish_date
                   AND sit.TYPE_FLAG IN (1, 2, 3)
                   AND cal.state = 'NSW' -- just grabbed one so we get one half hour only
                   AND sit.siteid = required_dates.siteid(+)
                   AND required_dates.siteid IS NULL
                   AND EXISTS
                          (SELECT 1
                             FROM tp_snap_company_identifier
                            WHERE     FRMP = nmi.FRMP
                                  AND snapshot_id = in_snapshot_id
                                  AND v_this_day BETWEEN OE_START_DATE
                                                     AND OE_FINISH_DATE)
                   AND cid_prices.state || '-' || sit.business_unit =
                          con.CONTRACT_NAME
                   AND con.contract_type = 'CIDSUMMARY'
                   AND cid_prices.DEFAULT_TARIFF_CODE =
                          ctp.DEFAULT_TARIFF_CODE
                   AND sit.SITEID = ctp.SITEID;

         log_message (
               'tp_agg_site_hh CID INSERT COUNT'
            || SQL%ROWCOUNT
            || ' day:'
            || v_this_day,
            'info');

         v_this_day := v_this_day + 1;
         COMMIT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;

   PROCEDURE Create_CID_Records (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE)
   IS
      v_affected_count     NUMBER;
      v_this_day           DATE;
      v_snapshot_type      VARCHAR2 (100);

      v_site_ids           number_list;
      v_default_codes      varchar2_list;
      v_last_month_count   NUMBER;
   BEGIN
      v_this_day := in_start_date;


      EXECUTE IMMEDIATE 'truncate table tp_snap_default_tariff';

      EXECUTE IMMEDIATE 'truncate table tp_cid_code_temp';

      INSERT INTO tp_snap_default_tariff
         SELECT DISTINCT default_tariff_code, STATE
           FROM tp_snap_cid_tariffs dt
          WHERE     default_tariff_type = 'GENERIC'
                AND superceded = 0
                AND snapshot_id = in_snapshot_id;


      INSERT INTO tp_cid_code_temp
         WITH days
              AS (    SELECT TRUNC (in_start_date) + LEVEL - 1 day
                        FROM DUAL
                  CONNECT BY LEVEL <= 1 + (in_finish_date - in_start_date))
         SELECT DISTINCT
                wva.siteid,
                --Get_Default_Tariff_O (wva.siteid,  d.day, in_snapshot_id) default_tariff_code
                NVL (wva.DEFAULT_CODE_OVERRIDE, tf.default_tariff_code)
           FROM tp_snap_site_table wva
                JOIN days d
                   ON d.day BETWEEN wva.start_date AND wva.finish_date
                JOIN tp_snap_default_tariff tf ON wva.state = tf.state;

      -- TODO - look at removing loop - test server is pretty poor so required for this environment

      -- now insert what should be populated
      WHILE (v_this_day <= in_finish_date)
      LOOP
         -- quicker to calc all this in one go and post to temporary table
         -- significantly faster + we get a lot of latch locks otherwise



         -- now insert what should be populated
         INSERT INTO tp_agg_site_hh (SNAPSHOT_ID,
                                     SITEID,
                                     CONTRACT_ID,
                                     REV,
                                     NMI,
                                     STATE,
                                     DATETIME,
                                     DT,
                                     MONTH_NUM,
                                     PEAK_FLAG,
                                     QTY,
                                     DEMAND,
                                     LP_STATUS,
                                     PRICE)
            WITH required_dates
                 AS (SELECT wva.start_date, wva.finish_date, siteid
                       FROM tp_snap_retail_site wva,
                            tp_snap_contract_complete con
                      WHERE     con.snapshot_id = in_snapshot_id
                            AND wva.snapshot_id = in_snapshot_id
                            AND built = 1
                            AND v_this_day BETWEEN wva.start_date
                                               AND wva.finish_date
                            AND con.contract_id = wva.contract_id
                            AND con.rev = wva.rev)
            SELECT                      /* USE_HASH(nmi sit cal cid_prices) */
                  in_snapshot_id,
                   sit.siteid,
                   con.contract_id,
                   con.rev,
                   nmi.nmi,
                   sit.state,
                   cal.datetime,
                   dt,
                   TO_CHAR ( (cal.datetime - 1 / 48), 'MM') month_num,
                   cal.peak_flag,
                   0,
                   0,
                   0,
                   cid_prices.cid_price
              FROM tp_snap_nmi nmi,
                   tp_snap_site_table sit,
                   tp_snap_calendar_hh cal,
                   tp_snap_cid_prices cid_prices,
                   tp_snap_contracts con,
                   required_dates,
                   tp_cid_code_temp ctp
             WHERE     nmi.SNAPSHOT_ID = in_snapshot_id
                   AND sit.snapshot_id = in_snapshot_id
                   AND cal.snapshot_id = in_snapshot_id
                   AND con.snapshot_id = in_snapshot_id
                   AND cal.dt = v_this_day --between trunc(in_month,'MON') and last_day(in_month)
                   --and     cal.dt = cal.datetime - 1 -- remove this for 48 hh records
                   AND cid_prices.state = sit.state
                   AND cid_prices.datetime = cal.datetime
                   AND cid_prices.snapshot_id = in_snapshot_id
                   AND sit.nmi = nmi.nmi
                   AND cal.dt BETWEEN sit.start_date AND sit.finish_date
                   AND cal.dt BETWEEN nmi.start_date AND nmi.finish_date
                   AND sit.TYPE_FLAG IN (1, 2, 3)
                   AND cal.state = 'NSW' -- just grabbed one so we get one half hour only
                   AND sit.siteid = required_dates.siteid(+)
                   AND required_dates.siteid IS NULL
                   AND EXISTS
                          (SELECT 1
                             FROM tp_snap_company_identifier
                            WHERE     FRMP = nmi.FRMP
                                  AND snapshot_id = in_snapshot_id
                                  AND v_this_day BETWEEN OE_START_DATE
                                                     AND OE_FINISH_DATE)
                   AND cid_prices.state || '-' || sit.business_unit =
                          con.CONTRACT_NAME
                   AND con.contract_type = 'CIDSUMMARY'
                   AND cid_prices.DEFAULT_TARIFF_CODE =
                          ctp.DEFAULT_TARIFF_CODE
                   AND sit.SITEID = ctp.SITEID;

         log_message (
               'tp_agg_site_hh CID INSERT COUNT'
            || SQL%ROWCOUNT
            || ' day:'
            || v_this_day,
            'info');

         v_this_day := v_this_day + 1;
         COMMIT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;


   PROCEDURE cleanup_act
   IS
   BEGIN
      UPDATE /*+ full(a) */
            tp_agg_site_hh a
         SET state = 'ACT'
       WHERE nmi IN (SELECT DISTINCT nmi
                       FROM tp_snap_nmi nmi
                      WHERE state = 'ACT');

      log_message ('tp_agg_site_hh ACT cleanup COUNT' || SQL%ROWCOUNT,
                   'info');
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;


   PROCEDURE gather_stats(in_table_name in varchar2) 
   IS
   BEGIN
      
            log_message ('COLLECT ORACLE STATS:' || in_table_name, 'info');

            SYS.DBMS_STATS.GATHER_TABLE_STATS (
               OwnName            => 'TEMP_DATA',
               TabName            => in_table_name,
               Estimate_Percent   => 2,
               Method_Opt         => 'FOR ALL COLUMNS SIZE 1',
               Degree             => 4,
               Cascade            => TRUE,
               No_Invalidate      => FALSE);
      
   END;
   
   
   
   PROCEDURE gather_stats
   IS
   BEGIN
      FOR REC IN (SELECT TABLE_NAME
                    FROM user_tables
                   WHERE table_name LIKE 'TP_SNAP%')
      LOOP
         

         gather_stats(in_table_name =>rec.table_name); 
      END LOOP;
   END;

   PROCEDURE apply_meterdata (in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE)
   IS
   BEGIN
      log_message ('wipe tp_agg_site_hh_temp ', 'info');

      EXECUTE IMMEDIATE 'truncate table tp_agg_site_hh_temp';

      INSERT INTO tp_agg_site_hh_temp (SNAPSHOT_ID,
                                       SITEID,
                                       CONTRACT_ID,
                                       REV,
                                       NMI,
                                       STATE,
                                       DATETIME,
                                       DT,
                                       MONTH_NUM,
                                       PEAK_FLAG,
                                       QTY,
                                       DEMAND,
                                       LP_STATUS)
         SELECT                                          /* PARALLEL(agg,4) */
               agg.SNAPSHOT_ID,
                agg.SITEID,
                agg.CONTRACT_ID,
                agg.REV,
                agg.NMI,
                agg.STATE,
                agg.DATETIME,
                agg.DT,
                agg.MONTH_NUM,
                agg.PEAK_FLAG,
                NVL (met.SETTLED / 2, 0),
                NVL (met.DEMAND / 2, 0),
                NVL2 (met.DEMAND, 0, 1)
           FROM tp_agg_site_hh agg,
                (SELECT *
                   FROM tp_snap_sitemeterdata st
                  WHERE     st.datetime > in_start_date
                        AND st.datetime <= in_finish_date + 1) met
          WHERE     agg.snapshot_id = in_snapshot_id
                AND agg.datetime = met.datetime(+)
                AND agg.siteid = met.siteid(+);


      reload_agg_site_hh ('METER_DATA');

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;



   PROCEDURE apply_load_profile (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE)
   IS
   
   BEGIN
      log_message ('wipe tp_load_profile_temp ', 'info');
      
       EXECUTE IMMEDIATE 'drop table tp_load_profile_temp';
      
--      begin
--         execute immediate 'drop index TP_LOAD_PROFILE_TEMP_IDX1';
--      exception when others then null;
--      end;

     
      
      EXECUTE IMMEDIATE 'create table tp_load_profile_temp as select * from tp_load_profile_tmp where 1=0';
        -- EXECUTE IMMEDIATE 'truncate table tp_load_profile_tmp';



      INSERT INTO tp_load_profile_temp (SNAPSHOT_ID,
                                        SITEID,
                                        DATETIME,
                                        DT,
                                        MONTH_NUM,
                                        period)
         SELECT snapshot_id,
                siteid,
                datetime,
                dt,
                month_num,
                TO_CHAR (dt, 'MON')
           FROM tp_agg_site_hh agg
          WHERE agg.LP_status = 1 AND agg.SNAPSHOT_ID = in_snapshot_id;
          
          log_message ('load tp_load_profile_temp rows: '||sql%rowcount, 'info');
          
          commit;
          
          
          
--            INSERT INTO tp_load_profile_temp (SNAPSHOT_ID,
--                                        SITEID,
--                                        DATETIME,
--                                        DT,
--                                        MONTH_NUM,
--                                        period)
--             select SNAPSHOT_ID,
--                                        SITEID,
--                                        DATETIME,
--                                        DT,
--                                        MONTH_NUM,
--                                        period
--             from tp_load_profile_tmp;
--             
--            log_message ('load tp_load_profile_temp rows: '||sql%rowcount, 'info');

           
--      
--      
--       begin
--         execute immediate 'CREATE INDEX TP_LOAD_PROFILE_TEMP_IDX1 ON TP_LOAD_PROFILE_TEMP(SNAPSHOT_ID, SITEID, DATETIME) nologging';
--         exception 
--         when others then null;
--      end;
      
      gather_stats('TP_LOAD_PROFILE_TEMP');
      
      

      EXECUTE IMMEDIATE 'truncate table tp_agg_site_hh_temp';

      INSERT INTO tp_agg_site_hh_temp
         SELECT                                          /* PARALLEL(agg,4) */
               agg.SNAPSHOT_ID,
                agg.SNAPSHOT_TYPE,
                agg.SITEID,
                agg.CONTRACT_ID,
                agg.REV,
                agg.NMI,
                agg.STATE,
                agg.DATETIME,
                agg.DT,
                agg.MONTH_NUM,
                agg.PEAK_FLAG,
                CASE (agg.LP_STATUS)
                   WHEN 1 THEN NVL (lpr.quantity_s, 0)
                   ELSE agg.QTY
                END,
                CASE (agg.LP_STATUS)
                   WHEN 1 THEN NVL (lpr.quantity_m, 0)
                   ELSE agg.demand
                END,
                agg.LP_STATUS
           FROM tp_agg_site_hh agg, V_TP_LOAD_PROFILE_TEMP lpr
          WHERE     agg.snapshot_id = in_snapshot_id
                AND agg.datetime = lpr.datetime(+)
                AND agg.siteid = lpr.siteid(+);

      reload_agg_site_hh ('LOAD_PROFILE');


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack, 'error');
         ROLLBACK;
   END;

   PROCEDURE agg_mass_market (in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE)
   IS
   BEGIN
   
      /* Mass market calculation combines ACT volume into NSW volume and includes SA ALTONA volume.   No FPP, NORSKE volumes is included */
   
      delete tp_agg_site_hh_state_result where snapshot_id = in_snapshot_id;
    
      delete tp_hh_category_result where snapshot_id = in_snapshot_id;
      
      insert into tp_agg_site_hh_state_result(SNAPSHOT_ID, STATE, DATETIME,  QUANTITY_MWH)
      select snapshot_id,state,datetime,sum(qty) from 
      (select snapshot_id,case state when 'ACT' then 'NSW' else  state end state,datetime,qty   
      from TEMP_DATA.TP_AGG_SITE_HH where snapshot_id = in_snapshot_id 
      --AND STATE IN ('QLD','VIC','SA')
--      union all
--      select snapshot_id,  'NSW' c_state, datetime,qty   
--      from TEMP_DATA.TP_AGG_SITE_HH where snapshot_id = in_snapshot_id AND STATE IN ('NSW','ACT')
      union all
      select 
      -- Included SA ALTONA volume 
       snapshot_id,
                     state,
                     md.datetime,
                     quantity_mwh
                FROM TEMP_DATA.TP_ADDITIONAL_VOLUME md where snapshot_id = in_snapshot_id
      )
      group by   snapshot_id,state,datetime;
      
      
      log_message ('calculate mass market.', 'info');
      
      insert into tp_hh_category_result(SNAPSHOT_ID,STATE, DATETIME, category_ID,  MWH)
      with ci_vol as 
      (
      select snapshot_id,  state,datetime,pkg_tp_snapshot_extra.g_ci_category, QUANTITY_MWH ci_mwh from TEMP_DATA.tp_agg_site_hh_state_result 
      where snapshot_id = in_snapshot_id 
      ),
      setcp as 
      (select snapshot_id,  state,datetime,pkg_tp_snapshot_extra.g_setcp_category, sum(mw)/2 setcp_mwh from tp_snap_setcpdata_t where snapshot_id = in_snapshot_id
      group by snapshot_id, state,datetime),
      ppa_vol as (select snapshot_id,  state,datetime,pkg_tp_snapshot_extra.g_ppa_category,sum(quantity_mw)/2 ppa_mwh 
      from TEMP_DATA.TP_SNAP_PPA_HH where snapshot_id = in_snapshot_id
      and name IN (SELECT c_value
                                      FROM tp_parameters
                                     WHERE name = 'PPA' AND active = 'Y')
      group by snapshot_id, state,datetime)
      ,MM_V AS (
      select ci.snapshot_id, ci.state,ci.datetime,pkg_tp_snapshot_extra.g_mm_category, nvl(((s.setcp_mwh+NVL(p.ppa_mwh,0)) - nvl(ci.ci_mwh,0)),0) mm_mwh
      from ci_vol ci left join setcp s on ci.state=s.state and ci.datetime = s.datetime
      LEFT join ppa_vol p on ci.state = p.state and ci.datetime = p.datetime)
      select * from ci_vol
      union all
      select * from setcp
      union all
      select * from ppa_vol
      union all
      select * from mm_v;
      

      
      log_message ('calculate mass market rowcount:'||sql%rowcount, 'info');

      COMMIT;
   END;

   PROCEDURE get_mm_volume_hh (in_snapshot_id     IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
        select STATE, DATETIME, CI_MWH, SETCP_MWH, PPA_MWH, MM_MWH, SNAPSHOT_ID
        from TEMP_DATA.TP_MM_ACTUAL_HH_RESULT where snapshot_id =in_snapshot_id; 
   END;
   
   PROCEDURE get_mm_volume_month (in_snapshot_id     IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
        select STATE, trunc(DATETIME-1/48,'MON'), sum(CI_MWH) ci_mwh, sum(SETCP_MWH) SETCP_MWH, sum(PPA_MWH) PPA_MWH, sum(MM_MWH) MM_MWH, SNAPSHOT_ID
        from TEMP_DATA.TP_MM_ACTUAL_HH_RESULT where snapshot_id =in_snapshot_id
        group by trunc(DATETIME-1/48,'MON'),snapshot_id; 
   END;
   
   
   PROCEDURE get_agg_site_hh_state (in_snapshot_id     IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
      select SNAPSHOT_ID, STATE, DATETIME, QUANTITY_MWH
      from TEMP_DATA.TP_AGG_SITE_HH_STATE_RESULT
        where snapshot_id =in_snapshot_id; 
   END;


   PROCEDURE get_ci_volume (p_snapshot_id      IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
         WITH counterparty
              AS (SELECT cp_id,
                         cp_name,
                         CASE WHEN cp_id IN (-1, 242) THEN 0 ELSE 1 END
                            cp_flag,
                         cp_institution_type
                    FROM tp_snap_counterparty
                   WHERE snapshot_id = p_snapshot_id),
              agg_contract_day
              AS (  SELECT a.snapshot_id,
                           a.state,
                           c.CONTRACT_TYPE,
                           c.CONTRACT_TYPE contract_sub_type,
                           c.CONTRACT_ID,
                           c.rev,
                           a.DT,
                           a.month_num,
                           a.PEAK_FLAG,
                           s.LNSP,
                           s.FRMP,
                           SUM (a.QTY) qty
                      FROM tp_agg_site_hh a,
                           tp_snap_contracts c,
                           tp_snap_site_table s
                     WHERE     a.CONTRACT_ID = c.CONTRACT_ID
                           AND c.built = 1
                           AND a.REV = c.REV
                           AND a.SNAPSHOT_ID = p_snapshot_id
                           AND c.SNAPSHOT_ID = a.SNAPSHOT_ID
                           AND s.SNAPSHOT_ID = a.SNAPSHOT_ID
                           AND a.SITEID = s.siteid
                           AND a.DT BETWEEN s.START_DATE AND s.FINISH_DATE
                  GROUP BY a.snapshot_id,
                           a.SNAPSHOT_TYPE,
                           a.state,
                           c.CONTRACT_TYPE,
                           c.CONTRACT_ID,
                           c.rev,
                           a.dt,
                           a.month_num,
                           a.PEAK_FLAG,
                           s.LNSP,
                           s.FRMP),
              v_agg_journal_month
              AS (  SELECT d.snapshot_id,
                           CASE cp.cp_institution_type
                              WHEN 'ERM CUSTOMER'
                              THEN
                                 'ERM CUSTOMER'
                              ELSE
                                 CASE d.contract_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                       'RETAIL'
                                    WHEN 'CIDSUMMARY'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE '%RETAIL%'
                                          THEN
                                             'RETAIL'
                                          WHEN c.contract_name LIKE '%ERM%'
                                          THEN
                                             'ERM'
                                          ELSE
                                             'WHOLESALE'
                                       END
                                    WHEN 'WVA'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE 'WHO%'
                                          THEN
                                             'WHOLESALE'
                                          ELSE
                                             'RETAIL'
                                       END
                                    ELSE
                                       c.contract_name
                                 END
                           END
                              business_unit,
                           CASE
                              WHEN d.contract_type IN ('CIDSUMMARY',
                                                       'MASS_MARKET')
                              THEN
                                 DECODE (d.frmp,
                                         'ENERGEX', 'SUNRETAIL',
                                         'CITIP', 'CITIPOWER',
                                         c.portfolio)
                              WHEN c.portfolio = 'RETAIL'
                              THEN
                                 'POWERCOR'
                              ELSE
                                 c.portfolio
                           END
                              portfolio,
                           d.state,
                           d.contract_type,
                           d.contract_sub_type,
                           TRUNC (dt, 'MON') dt,
                           peak_flag,
                           frmp,
                           SUM (qty) qty
                      FROM agg_contract_day d,
                           tp_snap_contracts c,
                           tp_snap_counterparty cp
                     WHERE     c.snapshot_id = d.snapshot_id
                           AND c.built = 1
                           AND c.contract_id = d.contract_id
                           AND c.cp_id = cp.cp_id(+)
                  GROUP BY d.snapshot_id,
                           CASE cp.cp_institution_type
                              WHEN 'ERM CUSTOMER'
                              THEN
                                 'ERM CUSTOMER'
                              ELSE
                                 CASE d.contract_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                       'RETAIL'
                                    WHEN 'CIDSUMMARY'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE
                                                  '%RETAIL%'
                                          THEN
                                             'RETAIL'
                                          WHEN c.contract_name LIKE '%ERM%'
                                          THEN
                                             'ERM'
                                          ELSE
                                             'WHOLESALE'
                                       END
                                    WHEN 'WVA'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE 'WHO%'
                                          THEN
                                             'WHOLESALE'
                                          ELSE
                                             'RETAIL'
                                       END
                                    ELSE
                                       c.contract_name
                                 END
                           END,
                           CASE
                              WHEN d.contract_type IN ('CIDSUMMARY',
                                                       'MASS_MARKET')
                              THEN
                                 DECODE (d.frmp,
                                         'ENERGEX', 'SUNRETAIL',
                                         'CITIP', 'CITIPOWER',
                                         c.portfolio)
                              WHEN c.portfolio = 'RETAIL'
                              THEN
                                 'POWERCOR'
                              ELSE
                                 c.portfolio
                           END,
                           d.state,
                           d.contract_type,
                           d.contract_sub_type,
                           TRUNC (dt, 'MON'),
                           peak_flag,
                           frmp),
              final_query
              AS (  SELECT CASE FRMP WHEN 'CRNGY' THEN 'CNRGY' ELSE FRMP END
                              FRMP,
                           CASE
                              WHEN CONTRACT_TYPE = 'CIDSUMMARY'
                              THEN
                                 CASE FRMP
                                    WHEN 'ENERGEX' THEN 'SUNRETAIL'
                                    WHEN 'CITIP' THEN 'CITIPOWER'
                                    ELSE frmp
                                 END
                              ELSE
                                 PORTFOLIO
                           END
                              PORTFOLIO,
                           STATE,
                           business_unit BU_FLAG,
                           CASE contract_type
                              WHEN 'RETAIL'
                              THEN
                                 'CandI'
                              WHEN 'CID'
                              THEN
                                 'Defaults'
                              WHEN 'CIDSUMMARY'
                              THEN
                                 'Defaults'
                              WHEN 'MASS_MARKET'
                              THEN
                                 CASE contract_sub_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                          'Franchise - '
                                       || DECODE (portfolio,
                                                  'POWERCOR', 'PCOR',
                                                  'CITIPOWER', 'CP',
                                                  'SUNRETAIL', 'ENGX',
                                                  portfolio)
                                       || ' MASS MKT'
                                    WHEN 'BASIC_WINS'
                                    THEN
                                       'Franchise - Basic Wins'
                                    ELSE
                                       contract_sub_type
                                 END
                              ELSE
                                 contract_type
                           END
                              Category,
                           ROUND (SUM (DECODE (peak_flag, 1, QTY, 0)), 5)
                              LOAD_PEAK,
                           ROUND (SUM (DECODE (peak_flag, 0, QTY)), 5)
                              LOAD_OFFPEAK,
                           a.dt AS period
                      FROM v_agg_journal_month a
                     WHERE (   (    FRMP <> 'CNRGY'
                                AND FRMP <> 'PACPOWER'
                                AND FRMP <> 'CRNGY'
                                AND FRMP <> 'INTLENGY')
                            OR (A.DT > '28-Feb-11'))
                  GROUP BY a.DT,
                           portfolio,
                           a.contract_type,
                           a.contract_sub_type,
                           state,
                           FRMP,
                           business_unit
                  ORDER BY a.DT,
                           portfolio,
                           a.contract_type,
                           a.contract_sub_type,
                           state,
                           business_unit)
         SELECT *
           FROM final_query;
   END;

   PROCEDURE get_ci_volume_netsrepp (p_snapshot_id      IN     NUMBER,
                                     out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      --      OPEN out_load_request FOR
      --         WITH counterparty
      --              AS (SELECT cp_id,
      --                         cp_name,
      --                         CASE WHEN cp_id IN (-1, 242) THEN 0 ELSE 1 END
      --                            cp_flag,
      --                         cp_institution_type
      --                    FROM tp_snap_counterparty_ntp
      --                   WHERE snapshot_id = p_snapshot_id),
      --              agg_contract_day
      --              AS (  SELECT a.snapshot_id,
      --                           a.state,
      --                           c.CONTRACT_TYPE,
      --                           c.CONTRACT_TYPE contract_sub_type,
      --                           c.CONTRACT_ID,
      --                           c.rev,
      --                           a.DT,
      --                           a.month_num,
      --                           a.PEAK_FLAG,
      --                           s.LNSP,
      --                           s.FRMP,
      --                           SUM (a.QTY) qty
      --                      FROM tp_agg_site_hh_ntp a,
      --                           tp_snap_contracts_ntp c,
      --                           tp_snap_site_table_ntp s
      --                     WHERE     a.CONTRACT_ID = c.CONTRACT_ID
      --                           AND c.built = 1
      --                           AND a.REV = c.REV
      --                           AND a.SNAPSHOT_ID = p_snapshot_id
      --                           AND c.SNAPSHOT_ID = a.SNAPSHOT_ID
      --                           AND s.SNAPSHOT_ID = a.SNAPSHOT_ID
      --                           AND a.SITEID = s.siteid
      --                           AND a.DT BETWEEN s.START_DATE AND s.FINISH_DATE
      --                  GROUP BY a.snapshot_id,
      --                           a.SNAPSHOT_TYPE,
      --                           a.state,
      --                           c.CONTRACT_TYPE,
      --                           c.CONTRACT_ID,
      --                           c.rev,
      --                           a.dt,
      --                           a.month_num,
      --                           a.PEAK_FLAG,
      --                           s.LNSP,
      --                           s.FRMP),
      --              v_agg_journal_month
      --              AS (  SELECT d.snapshot_id,
      --                           CASE cp.cp_institution_type
      --                              WHEN 'ERM CUSTOMER'
      --                              THEN
      --                                 'ERM CUSTOMER'
      --                              ELSE
      --                                 CASE d.contract_type
      --                                    WHEN 'MASS_MARKET'
      --                                    THEN
      --                                       'RETAIL'
      --                                    WHEN 'CIDSUMMARY'
      --                                    THEN
      --                                       CASE
      --                                          WHEN c.contract_name LIKE '%RETAIL%'
      --                                          THEN
      --                                             'RETAIL'
      --                                          WHEN c.contract_name LIKE '%ERM%'
      --                                          THEN
      --                                             'ERM'
      --                                          ELSE
      --                                             'WHOLESALE'
      --                                       END
      --                                    WHEN 'WVA'
      --                                    THEN
      --                                       CASE
      --                                          WHEN c.contract_name LIKE 'WHO%'
      --                                          THEN
      --                                             'WHOLESALE'
      --                                          ELSE
      --                                             'RETAIL'
      --                                       END
      --                                    ELSE
      --                                       c.contract_name
      --                                 END
      --                           END
      --                              business_unit,
      --                           CASE
      --                              WHEN d.contract_type IN ('CIDSUMMARY',
      --                                                       'MASS_MARKET')
      --                              THEN
      --                                 DECODE (d.frmp,
      --                                         'ENERGEX', 'SUNRETAIL',
      --                                         'CITIP', 'CITIPOWER',
      --                                         c.portfolio)
      --                              WHEN c.portfolio = 'RETAIL'
      --                              THEN
      --                                 'POWERCOR'
      --                              ELSE
      --                                 c.portfolio
      --                           END
      --                              portfolio,
      --                           d.state,
      --                           d.contract_type,
      --                           d.contract_sub_type,
      --                           TRUNC (dt, 'MON') dt,
      --                           peak_flag,
      --                           frmp,
      --                           SUM (qty) qty
      --                      FROM agg_contract_day d,
      --                           tp_snap_contracts_ntp c,
      --                           counterparty cp
      --                     WHERE     c.snapshot_id = d.snapshot_id
      --                           AND c.built = 1
      --                           AND c.contract_id = d.contract_id
      --                           AND c.cp_id = cp.cp_id(+)
      --                  GROUP BY d.snapshot_id,
      --                           CASE cp.cp_institution_type
      --                              WHEN 'ERM CUSTOMER'
      --                              THEN
      --                                 'ERM CUSTOMER'
      --                              ELSE
      --                                 CASE d.contract_type
      --                                    WHEN 'MASS_MARKET'
      --                                    THEN
      --                                       'RETAIL'
      --                                    WHEN 'CIDSUMMARY'
      --                                    THEN
      --                                       CASE
      --                                          WHEN c.contract_name LIKE
      --                                                  '%RETAIL%'
      --                                          THEN
      --                                             'RETAIL'
      --                                          WHEN c.contract_name LIKE '%ERM%'
      --                                          THEN
      --                                             'ERM'
      --                                          ELSE
      --                                             'WHOLESALE'
      --                                       END
      --                                    WHEN 'WVA'
      --                                    THEN
      --                                       CASE
      --                                          WHEN c.contract_name LIKE 'WHO%'
      --                                          THEN
      --                                             'WHOLESALE'
      --                                          ELSE
      --                                             'RETAIL'
      --                                       END
      --                                    ELSE
      --                                       c.contract_name
      --                                 END
      --                           END,
      --                           CASE
      --                              WHEN d.contract_type IN ('CIDSUMMARY',
      --                                                       'MASS_MARKET')
      --                              THEN
      --                                 DECODE (d.frmp,
      --                                         'ENERGEX', 'SUNRETAIL',
      --                                         'CITIP', 'CITIPOWER',
      --                                         c.portfolio)
      --                              WHEN c.portfolio = 'RETAIL'
      --                              THEN
      --                                 'POWERCOR'
      --                              ELSE
      --                                 c.portfolio
      --                           END,
      --                           d.state,
      --                           d.contract_type,
      --                           d.contract_sub_type,
      --                           TRUNC (dt, 'MON'),
      --                           peak_flag,
      --                           frmp)
      --           SELECT CASE FRMP WHEN 'CRNGY' THEN 'CNRGY' ELSE FRMP END FRMP,
      --                  CASE
      --                     WHEN CONTRACT_TYPE = 'CIDSUMMARY'
      --                     THEN
      --                        CASE FRMP
      --                           WHEN 'ENERGEX' THEN 'SUNRETAIL'
      --                           WHEN 'CITIP' THEN 'CITIPOWER'
      --                           ELSE frmp
      --                        END
      --                     ELSE
      --                        PORTFOLIO
      --                  END
      --                     PORTFOLIO,
      --                  STATE,
      --                  business_unit BU_FLAG,
      --                  CASE contract_type
      --                     WHEN 'RETAIL'
      --                     THEN
      --                        'CandI'
      --                     WHEN 'CID'
      --                     THEN
      --                        'Defaults'
      --                     WHEN 'CIDSUMMARY'
      --                     THEN
      --                        'Defaults'
      --                     WHEN 'MASS_MARKET'
      --                     THEN
      --                        CASE contract_sub_type
      --                           WHEN 'MASS_MARKET'
      --                           THEN
      --                                 'Franchise - '
      --                              || DECODE (portfolio,
      --                                         'POWERCOR', 'PCOR',
      --                                         'CITIPOWER', 'CP',
      --                                         'SUNRETAIL', 'ENGX',
      --                                         portfolio)
      --                              || ' MASS MKT'
      --                           WHEN 'BASIC_WINS'
      --                           THEN
      --                              'Franchise - Basic Wins'
      --                           ELSE
      --                              contract_sub_type
      --                        END
      --                     ELSE
      --                        contract_type
      --                  END
      --                     "Category",
      --                  ROUND (SUM (DECODE (peak_flag, 1, QTY, 0)), 5) LOAD_PEAK,
      --                  ROUND (SUM (DECODE (peak_flag, 0, QTY)), 5) LOAD_OFFPEAK,
      --                  a.dt AS period
      --             FROM v_agg_journal_month a
      --            WHERE --a.DT between :in_start_date and :in_finish_date and
      --                  (   (    FRMP <> 'CNRGY'
      --                       AND FRMP <> 'PACPOWER'
      --                       AND FRMP <> 'CRNGY'
      --                       AND FRMP <> 'INTLENGY')
      --                   OR (A.DT > '28-Feb-11'))
      --         --and contract_type = 'RETAIL'
      --         GROUP BY a.DT,
      --                  portfolio,
      --                  a.contract_type,
      --                  a.contract_sub_type,
      --                  state,
      --                  FRMP,
      --                  business_unit
      --         ORDER BY a.DT,
      --                  portfolio,
      --                  a.contract_type,
      --                  a.contract_sub_type,
      --                  state,
      --                  business_unit;
      NULL;
   END;


   PROCEDURE backup_netsrepp_snapshots (p_snapshot_id    IN NUMBER,
                                        in_start_date    IN DATE,
                                        in_finish_date   IN DATE)
   IS
   BEGIN
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_CONTRACTS_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_CONTRACT_COMPLETE_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_calendar_hh_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_cid_prices_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_cid_tariffs_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_sitemeterdata_ntp';
      --
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_HH_RETAIL_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_counterparty_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tp_snap_retail_site_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_RETAIL_COMPLETE_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_site_table_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_load_profile_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_nmi_ntp';
      --      EXECUTE IMMEDIATE 'truncate table TP_SNAP_regional_prices_ntp';
      --      EXECUTE IMMEDIATE 'truncate table tg_agg_site_hh_ntp';
      --
      --      insert into TEMP_DATA.TP_SNAP_CONTRACTS_ntp
      --      select * from retail.SNAP_CONTRACT@netsrepp.world where SNAPSHOT_ID = p_snapshot_id;
      --
      --      insert into TEMP_DATA.TP_SNAP_CONTRACT_COMPLETE
      --      select * from retail.SNAP_CONTRACT_COMPLETE@netsrepp.world where SNAPSHOT_ID = p_snapshot_id;
      --
      --
      --      insert into tp_snap_calendar_hh_ntp
      --      select * from retail.snap_calendar_hh@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into tp_snap_cid_prices_ntp
      --      select * from retail.snap_cid_prices@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into tp_snap_cid_tariffs_ntp
      --      select * from retail.snap_cid_tariffs@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into tp_snap_sitemeterdata_ntp
      --      select * from retail.snap_sitemeterdata@netsrepp.world where snapshot_id = p_snapshot_id and datetime > in_start_date and datetime <= in_finish_date+1;
      --
      --      insert into TEMP_DATA.TP_SNAP_HH_RETAIL_ntp
      --      select * from retail.SNAP_HH_RETAIL@netsrepp.world where SNAPSHOT_ID = p_snapshot_id and
      --      datetime > in_start_date and datetime <= in_start_date+1;
      --
      --      insert into tp_snap_counterparty_ntp
      --      select * from retail.snap_counterparty@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into tp_snap_retail_site_ntp
      --      select * from retail.snap_retail_site@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into TP_SNAP_RETAIL_COMPLETE_ntp
      --      select * from retail.snap_retail_complete@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into TP_SNAP_site_table_ntp
      --      select * from retail.snap_site_table@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into TP_SNAP_load_profile_ntp
      --      select * from retail.snap_load_profile@netsrepp.world where snapshot_id = p_snapshot_id and period_code=to_char(in_start_date,'MON');
      --
      --      insert into TP_SNAP_nmi_ntp
      --      select * from retail.snap_nmi@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into TP_SNAP_regional_prices_ntp
      --      select * from retail.snap_regional_prices@netsrepp.world where snapshot_id = p_snapshot_id;
      --
      --      insert into tp_agg_site_hh_ntp
      --      select * from retail.agg_site_hh@netsrepp.world where snapshot_id = p_snapshot_id and datetime > in_start_date and datetime <= in_finish_date+1;
      --

      COMMIT;
   END;


   PROCEDURE get_ppa_volume (p_snapshot_id      IN     NUMBER,
                             out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
           SELECT state,
                  name,
                  datetime,
                  SUM (quantity_mw) quantity_mw
             FROM TEMP_DATA.TP_SNAP_PPA_HH ppa
            WHERE   name IN (SELECT c_value
                                      FROM tp_parameters
                                     WHERE name = 'PPA' AND active = 'Y')
         GROUP BY state, name, datetime
         ORDER BY name, datetime;
   END;
   
  

   PROCEDURE additional_adjustment (in_snapshot_id   IN NUMBER,
                                    in_start_date    IN DATE,
                                    in_finish_date   IN DATE)
   IS
      v_actual_date     date;
      v_actual_pc        NUMBER;
   BEGIN
      log_message ('additional adjustment start_date:'||in_start_date || ' finish_date:'||in_finish_date, 'info');

      BEGIN
         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_DATA.TP_ADJUSTMENT_TEMP';

         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_DATA.TP_ADJUSTMENT';
         
         EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_DATA.TP_AGG_SITE_DAY';
         
         EXECUTE IMMEDIATE 'truncate table TEMP_DATA.TP_ADDITIONAL_VOLUME';
         
         INSERT INTO TP_AGG_SITE_DAY(SNAPSHOT_ID, MONTH_NUM, SITEID, DT, PEAK_FLAG, QTY, DEMAND, LP_AVG, STATE, NMI, FRMP, CONTRACT_ID, REV, LNSP)
         SELECT
         SNAPSHOT_ID, MONTH_NUM, SITEID, DT, PEAK_FLAG, QTY, DEMAND, LP_AVG, STATE, NMI, FRMP, CONTRACT_ID, REV, LNSP
         FROM V_AGG_SITE_DAY WHERE SNAPSHOT_ID = in_snapshot_id;
         
        

         INSERT INTO TEMP_DATA.TP_ADJUSTMENT_TEMP (snapshot_id,
                                                   FRMP,
                                                   STATE,
                                                   CONTRACT_NAME,
                                                   PORTFOLIO,
                                                   BUSINESS_UNIT,
                                                   LNSP,
                                                   TNI,
                                                   DEFAULT_TARIFF_CODE,
                                                   DESCRIPTION,
                                                   CONTRACT_TYPE,
                                                   CONTRACT_ID,
                                                   datetime,
                                                   SETTLED,
                                                   STATUS_PC,
                                                   SITE_ID,
                                                   NMI,
                                                   SITE,
                                                   TYPE_FLAG,
                                                   COUNTERPARTY)
              SELECT in_snapshot_id,
                     CASE asd.frmp WHEN 'CRNGY' THEN 'CNRGY' ELSE asd.frmp END
                        frmp,
                     asd.state,
                     sc.contract_name,
                     CASE
                        WHEN sc.CONTRACT_TYPE = 'CIDSUMMARY'
                        THEN
                           CASE asd.FRMP
                              WHEN 'ENERGEX' THEN 'SUNRETAIL'
                              WHEN 'CITIP' THEN 'CITIPOWER'
                              ELSE asd.FRMP
                           END
                        ELSE
                           sc.portfolio
                     END
                        PORTFOLIO,
                     sc.business_unit,
                     asd.lnsp,
                     sst.tni,
                     sst.derived_default_tariff_code AS default_tariff_code,
                       tp.description,
                  --   sc.derived_description description,
                     sc.contract_type,
                     sc.contract_id,
                     TRUNC (asd.dt, 'MON') Period_start,
                     ROUND (SUM (asd.qty), 5) settled,
                     1 - AVG (asd.lp_avg) Status_PC,
                     asd.siteid AS site_id,
                     asd.nmi,
                     sst.site,
                     sst.type_flag,
                     scp.cp_name
                FROM tp_snap_site_table sst
                     INNER JOIN TP_AGG_SITE_DAY asd
                        ON     sst.siteid = asd.siteid
                           AND asd.dt BETWEEN sst.start_date
                                          AND sst.finish_date
                     INNER JOIN tp_snap_contract_complete sc
                        ON     asd.contract_id = sc.contract_id
                           AND asd.rev = sc.rev
                     JOIN tp_snap_counterparty scp
                        ON     scp.snapshot_id = sst.snapshot_id
                           AND sc.cp_id = scp.cp_id
                           join TP_PARAMETERS tp on
                           scp.cp_id = tp.n_value
               WHERE     sst.snapshot_id = in_snapshot_id
                     AND asd.snapshot_id = in_snapshot_id
                     AND sc.snapshot_id = in_snapshot_id
                     and asd.dt >= in_start_date and asd.dt <= in_finish_date
                     AND sc.built = 1
                     AND asd.state IN ('NSW',
                                       'VIC',
                                       'QLD',
                                       'SA')
                     AND tp.NAME = 'COUNTERPARTY_ID'
                                             AND c_value = 'Y'
            GROUP BY asd.frmp,
                     asd.state,
                     sc.contract_name,
                     sc.portfolio,
                     sc.business_unit,
                     asd.lnsp,
                     sst.tni,
                     sst.derived_default_tariff_code,
                     sst.DEFAULT_CODE_OVERRIDE,
                     sc.contract_type,
                     sc.contract_id,
                     tp.description,
                     TRUNC (asd.dt, 'MON'),
                     month_num,
                     asd.siteid,
                     asd.nmi,
                     sst.site,
                     sst.type_flag,
                     scp.cp_name;


         INSERT INTO tp_adjustment (SNAPSHOT_ID,
                                    CATEGORY,
                                    LOAD_ACTUAL,
                                    DATETIME,
                                    STATE,
                                    LOAD_INC_FORECAST,
                                    LOAD_ADJUSTMENT,
                                    COUNTERPARTY)
            SELECT snapshot_id,
                   description,
                   settled,
                   datetime,
                   state,
                   setted_forecast,
                   setted_forecast - settled,
                   counterparty
              FROM (  SELECT snapshot_id,
                             counterparty,
                             datetime,
                             SUM (
                                CASE
                                   WHEN status_pc = 0 THEN settled
                                   ELSE settled / status_pc
                                END)
                                setted_forecast,
                             state,
                             description,
                             SUM (settled) settled
                        FROM tp_adjUSTMENT_temp
                       WHERE snapshot_id = in_snapshot_id
                    GROUP BY snapshot_id,
                             counterparty,
                             datetime,
                             state,
                             description
                             );

         log_message ('Adjustment COUNT:' || SQL%ROWCOUNT, 'info');
      EXCEPTION
         WHEN OTHERS
         THEN
          
            log_message (err_stack, 'error');
              raise_application_error (-20999, SQLERRM);
      END;

      COMMIT;

      /* Include ALTONA Adjustment */
      BEGIN
         SELECT                                         /*+ driving_site(md) */
               trunc(MAX(DATETIME-1/48))
           INTO v_actual_date
           FROM TP_METERDATA md
          WHERE     md.nmi10 = 'SRSWWRWTH1'
                AND md.day >= in_start_date
                AND md.day <= in_finish_date;

         -- work out the percentage actual to forecast
      v_actual_pc:=calculate_actual_percent(v_actual_date,in_finish_date);   


      insert into TEMP_DATA.TP_ADDITIONAL_VOLUME
      (SNAPSHOT_ID, CATEGORY, QUANTITY_MWH, DATETIME, STATE, LOAD_INC_FORECAST_MWH, LOAD_ADJUSTMENT_MWH, COUNTERPARTY)
       SELECT /*+ driving_site(md) */                                    
                    in_snapshot_id snapshot_id,
                    category,
                    load_actual,
                    datetime,
                    state,
                    load_forecast,
                    load_forecast,
                    counterparty from 
                    (
                    select
                     'ALTONA' category,
                     SUM (MD.E_ENERGY) / 1000 load_actual,
                     md.datetime,
                     'SA' state,
                     (SUM (MD.E_ENERGY) / NVL (v_actual_pc, 1)) / 1000
                        load_forecast,
                       (  (SUM (MD.E_ENERGY) / NVL (v_actual_pc, 1))
                        - SUM (MD.E_ENERGY)) 
                     / 1000 adj,
                     'ALTONA' counterparty
                FROM TP_METERDATA md
               WHERE     md.nmi10 = 'SRSWWRWTH1'
                     AND md.day >= in_start_date
                     AND md.day <= in_finish_date
                     group by md.datetime
                     );
            



         INSERT INTO tp_adjustment (SNAPSHOT_ID,
                                    CATEGORY,
                                    LOAD_ACTUAL,
                                    DATETIME,
                                    STATE,
                                    LOAD_INC_FORECAST,
                                    LOAD_ADJUSTMENT,
                                    COUNTERPARTY)
               
              SELECT                                     
                    snapshot_id,
                     category,
                     sum(quantity_mwh) load_actual,
                     TRUNC (md.datetime-1/48, 'MON') datetime,
                     state,
                     sum(load_inc_forecast_mwh),
                     sum(load_inc_forecast_mwh),
                     counterparty
                FROM tp_additional_volume md
            GROUP BY snapshot_id,
                     category,state,counterparty, TRUNC (md.datetime-1/48, 'MON')
            ;



         log_message ('ALTONA COUNT:' || SQL%ROWCOUNT, 'info');
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (-20999, SQLERRM);
            log_message (err_stack, 'error');
      END;
   END;



   PROCEDURE run_snapshot
   IS
      IN_SNAPSHOT_ID        NUMBER;
      IN_START_DATE         DATE;
      IN_FINISH_DATE        DATE;
      IN_SNAP_CONTRACT      VARCHAR2 (2) := 'N';
      IN_SNAP_REF_TABLES    VARCHAR2 (2) := 'N';
      IN_SNAP_HH_RETAIL     VARCHAR2 (2) := 'N';
      IN_SNAP_RETAIL_SITE   VARCHAR2 (2) := 'N';
      IN_SNAP_SMETER        VARCHAR2 (2) := 'N';
      IN_SNAP_LP            VARCHAR2 (2) := 'N';
      IN_AGG_CID            VARCHAR2 (2);
      IN_AGG_SITE_HH        VARCHAR2 (2);
      IN_APPLY_METERDATA    VARCHAR2 (2);
      IN_APPLY_LP           VARCHAR2 (2);
      IN_CLEANUP_ACT        VARCHAR2 (2);
      IN_GATHER_STATS       VARCHAR2 (2);
      V_SNAPSHOT_DATA       TP_PARAMETERS.C_VALUE%TYPE;
   BEGIN
      BEGIN
         SELECT TRUNC (d_value)
           INTO in_start_date
           FROM tp_parameters
          WHERE name = 'SNAPSHOT_START_DATE' AND ROWNUM <= 1;

         SELECT TRUNC (d_value)
           INTO in_FINISH_date
           FROM tp_parameters
          WHERE name = 'SNAPSHOT_FINISH_DATE' AND ROWNUM <= 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            IN_START_DATE := TRUNC (ADD_MONTHS (SYSDATE, -1), 'MON');
            in_FINISH_date := TRUNC (LAST_DAY (SYSDATE));
      END;

      BEGIN
         SELECT C_value
           INTO V_SNAPSHOT_DATA
           FROM tp_parameters
          WHERE name = 'SNAPSHOT_DATA' AND ROWNUM <= 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            V_SNAPSHOT_DATA := 'Y';
      END;
      
      update tp_snapshot set status = 'FAILED'  
      where status = 'RUNNING' and snapshot_start_date < trunc(sysdate)-1; 



      IN_SNAPSHOT_ID := TO_NUMBER (TO_CHAR (SYSDATE, 'YYYYMMDD'));
      
      insert into tp_snapshot(
      SNAPSHOT_ID, SNAPSHOT_START_DATE,  START_DATE, FINISH_DATE, SCHEDULED_BY, STATUS
      )
      values
      (IN_SNAPSHOT_ID,sysdate,IN_START_DATE,in_FINISH_date,user,'RUNNING');

      IF V_SNAPSHOT_DATA = 'Y'
      THEN
         IN_SNAP_CONTRACT := 'Y';
         IN_SNAP_REF_TABLES := 'Y';
         IN_SNAP_HH_RETAIL := 'Y';
         IN_SNAP_RETAIL_SITE := 'Y';
         IN_SNAP_SMETER := 'Y';
         IN_SNAP_LP := 'Y';
      END IF;

      IN_AGG_CID := 'Y';
      IN_AGG_SITE_HH := 'Y';
      IN_APPLY_METERDATA := 'Y';
      IN_APPLY_LP := 'Y';
      IN_CLEANUP_ACT := 'Y';
      IN_GATHER_STATS := 'Y';

      log_message (
            'Snapshot start '
         || IN_SNAPSHOT_ID
         || 'START_DATE:'
         || IN_START_DATE
         || ' FINISH_DATE:'
         || IN_FINISH_DATE,
         'info');
      TEMP_DATA.PKG_TP_SNAPSHOT.DO_SNAPSHOT (IN_SNAPSHOT_ID,
                                             IN_START_DATE,
                                             IN_FINISH_DATE,
                                             IN_GATHER_STATS,
                                             IN_SNAP_CONTRACT,
                                             IN_SNAP_REF_TABLES,
                                             IN_SNAP_HH_RETAIL,
                                             IN_SNAP_RETAIL_SITE,
                                             IN_SNAP_SMETER,
                                             IN_SNAP_LP,
                                             IN_AGG_CID,
                                             IN_AGG_SITE_HH,
                                             IN_APPLY_METERDATA,
                                             IN_APPLY_LP,
                                             IN_CLEANUP_ACT);
      log_message (
            'Snapshot finished '
         || IN_SNAPSHOT_ID
         || ' START_DATE:'
         || IN_START_DATE
         || ' FINISH_DATE:'
         || IN_FINISH_DATE,
         'info');
         
      UPDATE TP_SNAPSHOT SET STATUS = 'COMPLETE',SNAPSHOT_FINISH_DATE=sysdate, SNAPSHOT_RUN_DURATION=TO_NUMBER((sysdate-SNAPSHOT_START_DATE))*24 
      WHERE SNAPSHOT_ID =   IN_SNAPSHOT_ID; 
      COMMIT;
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
   
   

   PROCEDURE refresh_views(in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE)
   IS
   BEGIN
   
   
         DELETE TEMP_DATA.TP_AGG_SITE_MONTH_RESULT WHERE SNAPSHOT_ID = IN_SNAPSHOT_ID;      
         delete TEMP_DATA.TP_CI_ACTUAL_RESULT where snapshot_id = in_snapshot_id;
         delete TEMP_DATA.TP_ADJUSTMENT_RESULT where snapshot_id = in_snapshot_id;
         delete TEMP_DATA.TP_PPA_HH_RESULT where snapshot_id = in_snapshot_id;
         delete TEMP_DATA.TP_SETCPDATA_RESULT where snapshot_id = in_snapshot_id;
         
         insert into TEMP_DATA.TP_AGG_SITE_MONTH_RESULT(
         SNAPSHOT_ID, FRMP, PORTFOLIO, STATE, BU_FLAG, CATEGORY, LOAD_PEAK, LOAD_OFFPEAK, PERIOD,siteid)
         WITH counterparty
              AS (
                            SELECT snapshot_id,cp_id,
                         cp_name,
                         CASE WHEN cp_id IN (-1, 242) THEN 0 ELSE 1 END
                            cp_flag,
                         cp_institution_type
                    FROM tp_snap_counterparty
                   ),
              agg_contract_day
              AS (  SELECT a.snapshot_id,
                           a.state,
                           c.CONTRACT_TYPE,
                           c.CONTRACT_TYPE contract_sub_type,
                           c.CONTRACT_ID,
                           c.rev,
                           a.DT,
                           a.month_num,
                           a.PEAK_FLAG,
                           s.LNSP,
                           s.FRMP,
                           a.siteid,
                           SUM (a.QTY) qty
                      FROM tp_agg_site_hh a,
                           tp_snap_contracts c,
                           tp_snap_site_table s
                     WHERE     a.CONTRACT_ID = c.CONTRACT_ID
                           AND c.built = 1
                           AND a.REV = c.REV
                           AND c.SNAPSHOT_ID = a.SNAPSHOT_ID
                           AND s.SNAPSHOT_ID = a.SNAPSHOT_ID
                           AND a.SITEID = s.siteid
                           AND a.DT BETWEEN s.START_DATE AND s.FINISH_DATE
                  GROUP BY a.snapshot_id,
                           a.SNAPSHOT_TYPE,
                           a.state,
                           c.CONTRACT_TYPE,
                           c.CONTRACT_ID,
                           c.rev,
                           a.dt,
                           a.month_num,
                           a.PEAK_FLAG,
                           s.LNSP,
                           s.FRMP,
                           a.siteid),
              v_agg_journal_month
              AS (  SELECT d.snapshot_id,
                           CASE cp.cp_institution_type
                              WHEN 'ERM CUSTOMER'
                              THEN
                                 'ERM CUSTOMER'
                              ELSE
                                 CASE d.contract_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                       'RETAIL'
                                    WHEN 'CIDSUMMARY'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE '%RETAIL%'
                                          THEN
                                             'RETAIL'
                                          WHEN c.contract_name LIKE '%ERM%'
                                          THEN
                                             'ERM'
                                          ELSE
                                             'WHOLESALE'
                                       END
                                    WHEN 'WVA'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE 'WHO%'
                                          THEN
                                             'WHOLESALE'
                                          ELSE
                                             'RETAIL'
                                       END
                                    ELSE
                                       c.contract_name
                                 END
                           END
                              business_unit,
                           CASE
                              WHEN d.contract_type IN ('CIDSUMMARY',
                                                       'MASS_MARKET')
                              THEN
                                 DECODE (d.frmp,
                                         'ENERGEX', 'SUNRETAIL',
                                         'CITIP', 'CITIPOWER',
                                         c.portfolio)
                              WHEN c.portfolio = 'RETAIL'
                              THEN
                                 'POWERCOR'
                              ELSE
                                 c.portfolio
                           END
                              portfolio,
                           d.state,
                           d.contract_type,
                           d.contract_sub_type,
                           TRUNC (dt, 'MON') dt,
                           peak_flag,
                           frmp,
                           d.siteid,
                           SUM (qty) qty
                      FROM agg_contract_day d,
                           tp_snap_contracts c,
                           tp_snap_counterparty cp
                     WHERE     c.snapshot_id = d.snapshot_id
                           AND c.built = 1
                           AND c.contract_id = d.contract_id
                           AND c.cp_id = cp.cp_id(+)
                  GROUP BY d.snapshot_id,
                           CASE cp.cp_institution_type
                              WHEN 'ERM CUSTOMER'
                              THEN
                                 'ERM CUSTOMER'
                              ELSE
                                 CASE d.contract_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                       'RETAIL'
                                    WHEN 'CIDSUMMARY'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE
                                                  '%RETAIL%'
                                          THEN
                                             'RETAIL'
                                          WHEN c.contract_name LIKE '%ERM%'
                                          THEN
                                             'ERM'
                                          ELSE
                                             'WHOLESALE'
                                       END
                                    WHEN 'WVA'
                                    THEN
                                       CASE
                                          WHEN c.contract_name LIKE 'WHO%'
                                          THEN
                                             'WHOLESALE'
                                          ELSE
                                             'RETAIL'
                                       END
                                    ELSE
                                       c.contract_name
                                 END
                           END,
                           CASE
                              WHEN d.contract_type IN ('CIDSUMMARY',
                                                       'MASS_MARKET')
                              THEN
                                 DECODE (d.frmp,
                                         'ENERGEX', 'SUNRETAIL',
                                         'CITIP', 'CITIPOWER',
                                         c.portfolio)
                              WHEN c.portfolio = 'RETAIL'
                              THEN
                                 'POWERCOR'
                              ELSE
                                 c.portfolio
                           END,
                           d.state,
                           d.contract_type,
                           d.contract_sub_type,
                           TRUNC (dt, 'MON'),
                           peak_flag,
                           frmp,
                           d.siteid
                           ),
              final_query
              AS (  SELECT snapshot_id,  CASE FRMP WHEN 'CRNGY' THEN 'CNRGY' ELSE FRMP END
                              FRMP,
                           CASE
                              WHEN CONTRACT_TYPE = 'CIDSUMMARY'
                              THEN
                                 CASE FRMP
                                    WHEN 'ENERGEX' THEN 'SUNRETAIL'
                                    WHEN 'CITIP' THEN 'CITIPOWER'
                                    ELSE frmp
                                 END
                              ELSE
                                 PORTFOLIO
                           END
                              PORTFOLIO,
                           STATE,
                           business_unit BU_FLAG,
                           CASE contract_type
                              WHEN 'RETAIL'
                              THEN
                                 'CandI'
                              WHEN 'CID'
                              THEN
                                 'Defaults'
                              WHEN 'CIDSUMMARY'
                              THEN
                                 'Defaults'
                              WHEN 'MASS_MARKET'
                              THEN
                                 CASE contract_sub_type
                                    WHEN 'MASS_MARKET'
                                    THEN
                                          'Franchise - '
                                       || DECODE (portfolio,
                                                  'POWERCOR', 'PCOR',
                                                  'CITIPOWER', 'CP',
                                                  'SUNRETAIL', 'ENGX',
                                                  portfolio)
                                       || ' MASS MKT'
                                    WHEN 'BASIC_WINS'
                                    THEN
                                       'Franchise - Basic Wins'
                                    ELSE
                                       contract_sub_type
                                 END
                              ELSE
                                 contract_type
                           END
                              Category,
                           ROUND (SUM (DECODE (peak_flag, 1, QTY, 0)), 5)
                              LOAD_PEAK,
                           ROUND (SUM (DECODE (peak_flag, 0, QTY)), 5)
                              LOAD_OFFPEAK,
                           a.dt AS period,
                           a.siteid
                      FROM v_agg_journal_month a
                     WHERE 
                           (   (    FRMP <> 'CNRGY'
                                AND FRMP <> 'PACPOWER'
                                AND FRMP <> 'CRNGY'
                                AND FRMP <> 'INTLENGY') OR (A.DT > '28-Feb-11')
                            )
                  GROUP BY 
                  snapshot_id,a.DT,
                           portfolio,
                           a.contract_type,
                           a.contract_sub_type,
                           state,
                           FRMP,
                           business_unit,
                           a.siteid
                  ORDER BY a.DT,
                           portfolio,
                           a.contract_type,
                           a.contract_sub_type,
                           state,
                           business_unit
                           )
         SELECT *
           FROM final_query;

         insert into TEMP_DATA.TP_CI_ACTUAL_RESULT(
         SNAPSHOT_ID, FRMP, PORTFOLIO, STATE, BU_FLAG, CATEGORY, PERIOD, LOAD_PEAK, LOAD_OFFPEAK)
        select SNAPSHOT_ID, FRMP, PORTFOLIO, STATE, BU_FLAG, CATEGORY, PERIOD,
        sum(LOAD_PEAK) load_peak, sum(LOAD_OFFPEAK) load_offpeak from TEMP_DATA.TP_AGG_SITE_MONTH_RESULT
        WHERE SNAPSHOT_ID = in_snapshot_id
         group by SNAPSHOT_ID, FRMP, PORTFOLIO, STATE, BU_FLAG, CATEGORY, PERIOD;
           
        insert into TEMP_DATA.TP_ADJUSTMENT_RESULT(SNAPSHOT_ID, CATEGORY, LOAD_ACTUAL, DATETIME, STATE, LOAD_INC_FORECAST, LOAD_ADJUSTMENT, COUNTERPARTY)
        select SNAPSHOT_ID, CATEGORY, LOAD_ACTUAL, DATETIME, STATE, LOAD_INC_FORECAST, LOAD_ADJUSTMENT, COUNTERPARTY from TEMP_DATA.TP_ADJUSTMENT 
         where load_adjustment>0; 

      insert into TEMP_DATA.TP_PPA_HH_RESULT(NAME, STATE, DATETIME, QUANTITY_MW, SNAPSHOT_ID)
      SELECT name, state, datetime, SUM (quantity_mw) quantity_mw,snapshot_id
                      FROM TEMP_DATA.TP_SNAP_PPA_HH ppa
                     WHERE name IN (select c_value from tp_parameters where name = 'PPA' and active='Y') group by name,state,datetime,snapshot_id ;
      
      insert into TEMP_DATA.TP_SETCPDATA_RESULT( DATETIME, STATE, MW, MW_INC_FORECAST, MW_ADJUSTMENT, SNAPSHOT_ID)
       select DATETIME, STATE, MW, MW_INC_FORECAST, MW_ADJUSTMENT, in_SNAPSHOT_ID from TP_SNAP_SETCPDATA  ; 
       
       commit;

   END;
   
   PROCEDURE run_snapshot(in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE)
   IS
     IN_SNAP_CONTRACT VARCHAR2(2);
  IN_SNAP_REF_TABLES VARCHAR2(2);
  IN_SNAP_HH_RETAIL VARCHAR2(2);
  IN_SNAP_RETAIL_SITE VARCHAR2(2);
  IN_SNAP_SMETER VARCHAR2(2);
  IN_SNAP_LP VARCHAR2(2);
  IN_AGG_CID VARCHAR2(2);
  IN_AGG_SITE_HH VARCHAR2(2);
  IN_APPLY_METERDATA VARCHAR2(2);
  IN_APPLY_LP VARCHAR2(2);
  IN_CLEANUP_ACT VARCHAR2(2);
  IN_GATHER_STATS varchar2(2);
BEGIN
  IN_SNAP_CONTRACT := 'Y';
  IN_SNAP_REF_TABLES := 'Y';
  IN_SNAP_HH_RETAIL := 'Y';
  IN_SNAP_RETAIL_SITE := 'Y';
  IN_SNAP_SMETER := 'Y';
  IN_SNAP_LP := 'Y';
  IN_AGG_CID := 'Y';
  IN_AGG_SITE_HH := 'Y';
  IN_APPLY_METERDATA := 'Y';
  IN_APPLY_LP := 'Y';
  IN_CLEANUP_ACT := 'Y';
  IN_GATHER_STATS:='Y';
  
   TEMP_DATA.PKG_TP_SNAPSHOT.DO_SNAPSHOT ( IN_SNAPSHOT_ID, IN_START_DATE, IN_FINISH_DATE, IN_GATHER_STATS, IN_SNAP_CONTRACT, IN_SNAP_REF_TABLES, IN_SNAP_HH_RETAIL, IN_SNAP_RETAIL_SITE, IN_SNAP_SMETER, IN_SNAP_LP, IN_AGG_CID, IN_AGG_SITE_HH, IN_APPLY_METERDATA, IN_APPLY_LP, IN_CLEANUP_ACT );
commit;
      
   end;

   PROCEDURE schedule_job (p_start_date       IN     DATE,
                           out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      IF (p_start_date > TO_DATE ('01-jan-2000') or p_start_date is not null)
      THEN
         SYS.DBMS_SCHEDULER.SET_ATTRIBUTE (
            name        => 'TEMP_DATA.TP_CALC_MASS_MARKET',
            attribute   => 'START_DATE',
            VALUE       => TO_TIMESTAMP_TZ (
                                TO_CHAR (p_start_date,
                                         'YYYY/MM/DD HH24:MI:SS')
                             || '.000000 Australia/Sydney',
                             'yyyy/mm/dd hh24:mi:ss.ff tzr'));

         SYS.DBMS_SCHEDULER.ENABLE (name => 'TEMP_DATA.TP_CALC_MASS_MARKET');
      ELSE
         SYS.DBMS_SCHEDULER.DISABLE (name => 'TEMP_DATA.TP_CALC_MASS_MARKET');
      END IF;

      OPEN out_load_request FOR
         SELECT TO_CHAR (XX.START_DATE, 'DD/MM/YYYY HH24:MI')
                   NEW_MM_NEXT_RUN_DATE,
                STATE
           FROM USER_SCHEDULER_JOBS XX
          WHERE JOB_NAME = 'TP_CALC_MASS_MARKET';
   END;
END;
/