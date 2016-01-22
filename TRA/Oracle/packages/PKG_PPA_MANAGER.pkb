CREATE OR REPLACE PACKAGE BODY TRADINGANALYSIS.PKG_PPA_MANAGER
AS
   -- code owned by phil tao

   PROCEDURE P_CREATE_PPA_FCAST_HEADER (p_name       IN     VARCHAR2,
                                        p_fcast_id      OUT NUMBER,
                                        p_revision      OUT NUMBER)
   IS
      v_fcast_id   TRADINGANALYSIS.FCAST_HEADER.FCAST_ID%TYPE;
      v_revision   TRADINGANALYSIS.FCAST_HEADER.revision%TYPE;
   BEGIN
        SELECT fcast_id, MAX (revision)
          INTO V_FCAST_ID, v_revision
          FROM TRADINGANALYSIS.FCAST_HEADER
         WHERE     name = p_name
               AND duration = 'LONG_TERM'
               AND fcast_category = 'PPA'
      GROUP BY fcast_id;

      INSERT INTO TRADINGANALYSIS.FCAST_HEADER (FCAST_ID,
                                                REVISION,
                                                NAME,
                                                FCAST_CATEGORY,
                                                FCAST_TYPE,
                                                DURATION,
                                                STATE,
                                                LR,
                                                FCAST_BASE,
                                                START_DATE,
                                                FINISH_DATE,
                                                NETS_UPLOAD,
                                                MTM_UPLOAD,
                                                EXPOSURE_UPLOAD,
                                                NETS_CONTRACT_ID,
                                                COMMENTS,
                                                lastchanged)
         SELECT fcast_id,
                v_revision + 1,
                NAME,
                FCAST_CATEGORY,
                FCAST_TYPE,
                DURATION,
                STATE,
                LR,
                FCAST_BASE,
                START_DATE,
                FINISH_DATE,
                NETS_UPLOAD,
                MTM_UPLOAD,
                EXPOSURE_UPLOAD,
                NETS_CONTRACT_ID,
                COMMENTS,
                SYSDATE
           FROM TRADINGANALYSIS.FCAST_HEADER
          WHERE fcast_id = v_fcast_id AND revision = v_revision;

      p_fcast_id := v_fcast_id;
      p_revision := v_revision + 1;
   END;

   PROCEDURE P_CREATE_PPA_FCAST_HEADER (
      p_new_name         IN     VARCHAR2,
      p_state            IN     VARCHAR2,
      p_FCAST_CATEGORY   IN     VARCHAR2,
      p_FCAST_TYPE       IN     VARCHAR2,
      p_DURATION         IN     VARCHAR2 DEFAULT 'LONG_TERM',
      p_fcast_id            OUT NUMBER,
      p_revision            OUT NUMBER)
   IS
      v_fcast_id   NUMBER (5);
   BEGIN
      BEGIN
         SELECT MAX (fcast_id)
           INTO v_fcast_id
           FROM TRADINGANALYSIS.FCAST_HEADER fh
          WHERE     fh.fcast_category = p_FCAST_CATEGORY
                AND fh.fcast_type = p_FCAST_TYPE
                AND fh.duration = p_DURATION;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE;
      END;

      DBMS_OUTPUT.PUT_LINE (V_FCAST_ID);

      INSERT INTO TRADINGANALYSIS.FCAST_HEADER (FCAST_ID,
                                                REVISION,
                                                NAME,
                                                FCAST_CATEGORY,
                                                FCAST_TYPE,
                                                DURATION,
                                                STATE,
                                                LR,
                                                FCAST_BASE,
                                                START_DATE,
                                                FINISH_DATE,
                                                NETS_UPLOAD,
                                                MTM_UPLOAD,
                                                EXPOSURE_UPLOAD,
                                                NETS_CONTRACT_ID,
                                                COMMENTS,
                                                lastchanged)
         SELECT v_fcast_id + 1,
                1,
                p_new_name,
                FCAST_CATEGORY,
                FCAST_TYPE,
                DURATION,
                p_STATE,
                'ALL',
                FCAST_BASE,
                START_DATE,
                FINISH_DATE,
                NETS_UPLOAD,
                MTM_UPLOAD,
                EXPOSURE_UPLOAD,
                NETS_CONTRACT_ID,
                '',
                SYSDATE
           FROM TRADINGANALYSIS.FCAST_HEADER fh
          WHERE fcast_id = v_fcast_id AND revision = 1;

      p_fcast_id := v_fcast_id + 1;
      p_revision := 1;
   END;



   PROCEDURE P_SAVE (p_fcast_id     IN NUMBER,
                     p_revision     IN NUMBER,
                     p_expect_rec   IN NUMBER)
   IS
      v_name                   fcast_header.NAME%TYPE;
      v_count                  NUMBER (6);
      NO_RECORD                EXCEPTION;
      NOT_MATCH_EXPECTED_REC   EXCEPTION;
      PRAGMA EXCEPTION_INIT (NO_RECORD, -955);
      PRAGMA EXCEPTION_INIT (NOT_MATCH_EXPECTED_REC, -956);
   BEGIN
      BEGIN
         SELECT name
           INTO v_name
           FROM fcast_header
          WHERE fcast_id = p_fcast_id AND revision = p_revision;
      EXCEPTION
         WHEN OTHERS
         THEN
            DBMS_OUTPUT.put_line (
                  'fcast_header record '
               || p_fcast_id
               || ' '
               || p_revision
               || ' does not exist.');
            RAISE;
      END;

      SELECT COUNT (*) INTO v_count FROM ppa_detail_temp;

      IF v_count <> p_expect_rec
      THEN
         RAISE NOT_MATCH_EXPECTED_REC;
      END IF;

      IF v_count > 0
      THEN
         DBMS_OUTPUT.put_line ('expected rows insert ' || v_count);

         INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                      REVISION,
                                      DATETIME,
                                      QUANTITY_MW,
                                      RATE)
            SELECT p_FCAST_ID,
                   p_REVISION,
                   DATETIME,
                   QUANTITY_MW,
                   NVL (RATE, 0)
              FROM ppa_detail_temp;

         DBMS_OUTPUT.put_line ('rows inserted ' || SQL%ROWCOUNT);


         UPDATE TRADINGANALYSIS.FCAST_HEADER fh
            SET (fh.START_DATE, fh.FINISH_DATE) =
                   (SELECT TRUNC (MIN (datetime)), TRUNC (MAX (datetime))
                      FROM ppa_detail_temp)
          WHERE fcast_id = p_fcast_id AND revision = P_REVISION;
      ELSE
         DBMS_OUTPUT.put_line ('No row found in temp table ppa_detail_temp');
         RAISE NO_RECORD;
      END IF;
   END;

   PROCEDURE P_DELETE (p_fcast_id IN NUMBER, p_revision IN NUMBER)
   IS
   BEGIN
      DELETE FCAST_DETAIL_HH
       WHERE (fcast_id, revision) IN (SELECT fcast_id, revision
                                        FROM fcast_header
                                       WHERE     fcast_id = p_fcast_id
                                             AND revision = p_revision
                                             AND duration = 'LONG_TERM'
                                             AND fcast_category = 'PPA');

      DELETE fcast_header
       WHERE fcast_id = p_fcast_id AND revision = p_revision;
   END;


   PROCEDURE P_MAIN (p_name IN VARCHAR2, p_expect_rec IN NUMBER)
   IS
      v_fcast_id   TRADINGANALYSIS.FCAST_HEADER.FCAST_ID%TYPE;
      v_revision   TRADINGANALYSIS.FCAST_HEADER.revision%TYPE;
   BEGIN
      BEGIN
         P_CREATE_PPA_FCAST_HEADER (p_name, v_fcast_id, v_revision);
         P_SAVE (v_fcast_id, v_revision, p_expect_rec);
         P_CHECK (v_fcast_id, v_revision);
         COMMIT;
      EXCEPTION
         WHEN OTHERS
         THEN
            RAISE;
      END;
   END;



   PROCEDURE P_NEW_PPA (p_name         IN VARCHAR2,
                        p_state        IN VARCHAR2,
                        p_expect_rec   IN NUMBER)
   IS
      v_fcast_id   TRADINGANALYSIS.FCAST_HEADER.FCAST_ID%TYPE;
      v_revision   TRADINGANALYSIS.FCAST_HEADER.revision%TYPE;
   BEGIN
      BEGIN
         P_CREATE_PPA_FCAST_HEADER (p_name,
                                    p_state,
                                    'PPA',
                                    'AVERAGE',
                                    'LONG_TERM',
                                    v_fcast_id,
                                    v_revision);

         P_SAVE (v_fcast_id, v_revision, p_expect_rec);
         P_CHECK (v_fcast_id, v_revision);
         COMMIT;
      EXCEPTION
         WHEN OTHERS
         THEN
            RAISE;
      END;
   END;

   PROCEDURE P_CHECK (p_fcast_id IN NUMBER, p_revision IN NUMBER)
   IS
      v_start_date    DATE;
      v_finish_date   DATE;
      v_name          fcast_header.NAME%TYPE;
   BEGIN
      SELECT start_date, finish_date, name
        INTO v_start_date, v_finish_date, v_name
        FROM fcast_header
       WHERE fcast_id = p_fcast_id AND revision = p_revision;

      DBMS_OUTPUT.put_line (
            'NAME:'
         || v_NAME
         || ',FCAST_ID:'
         || p_FCAST_ID
         || ' REVISION:'
         || p_REVISION);
   END;


   PROCEDURE P_CREATE_RATES (p_ppa_id        IN NUMBER,
                             p_start_date    IN DATE,
                             p_finish_date   IN DATE,
                             p_rate          IN NUMBER,
                             p_rate_type     IN VARCHAR2)
   IS
      v_count      NUMBER (6);
      v_price_id   ppa_price_header.PRICE_ID%TYPE;
      v_name       VARCHAR2 (32);
   BEGIN
      SELECT name
        INTO v_name
        FROM ppa_master
       WHERE ppa_id = p_ppa_id;

      -- ensure that records does not exists
      SELECT COUNT (*)
        INTO v_count
        FROM ppa_master ms JOIN ppa_price_header ph ON ms.ppa_id = ph.ppa_id
       WHERE     ms.ppa_id = p_ppa_id
             AND ph.START_DATE = p_start_date
             AND ph.FINISH_DATE = p_finish_date;

      IF v_count = 0
      THEN
         INSERT INTO ppa_price_header (PRICE_ID,
                                       START_DATE,
                                       FINISH_DATE,
                                       ACTIVE,
                                       PPA_ID,
                                       DESCRIPTION)
              VALUES (seq_ppa_id.NEXTVAL,
                      p_start_date,
                      p_finish_date,
                      'Y',
                      p_ppa_id,
                      v_name)
           RETURNING price_id
                INTO v_price_id;

         INSERT INTO TRADINGANALYSIS.PPA_PRICE_DETAIL (PRICE_ID,
                                                       NAME,
                                                       DESCRIPTION,
                                                       RATE,
                                                       CALENDAR,
                                                       ACTIVE,
                                                       RATE_TYPE_ID)
            SELECT v_price_id,
                   pt.NAME,
                   NULL,
                   p_rate,
                   pt.CAL_NO,
                   'Y',
                   pt.RATE_TYPE_ID
              FROM PPA_PRICE_TYPE pt
             WHERE pt.name = p_rate_type;


         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;
   END;

   PROCEDURE P_UPDATE_RATE (p_ppa_name     IN VARCHAR2,
                            p_start_date   IN DATE,
                            p_rate         IN NUMBER,
                            p_rate_type    IN VARCHAR2)
   IS
      v_ppa_id   ppa_master.ppa_id%TYPE;
   BEGIN
      BEGIN
         SELECT ppa_id
           INTO v_ppa_id
           FROM ppa_master
          WHERE name = p_ppa_name;

         P_UPDATE_RATE (v_ppa_id,
                        p_start_date,
                        p_rate,
                        p_rate_type);
      EXCEPTION
         WHEN OTHERS
         THEN
            RAISE;
      END;
   END;

   PROCEDURE P_UPDATE_RATE (p_ppa_id       IN NUMBER,
                            p_start_date   IN DATE,
                            p_rate         IN NUMBER,
                            p_rate_type    IN VARCHAR2)
   -- assumption there is no overlapping start_date,finish_date
   IS
      v_count      NUMBER (6);
      v_price_id   ppa_price_header.PRICE_ID%TYPE;
      v_name       VARCHAR2 (32);
   BEGIN
      SELECT price_id
        INTO v_price_id
        FROM ppa_price_header
       WHERE ppa_id = p_ppa_id AND start_date = p_start_date;


      UPDATE TRADINGANALYSIS.PPA_PRICE_DETAIL
         SET rate = p_rate
       WHERE     PRICE_ID = v_price_id
             AND rate_type_id = (SELECT pt.RATE_TYPE_ID
                                   FROM PPA_PRICE_TYPE pt
                                  WHERE pt.name = p_rate_type);


      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         RAISE;
   END;


   FUNCTION create_fcast_revision (p_fcast_id   IN NUMBER,
                                   p_comment    IN VARCHAR2)
      -- This is the basic level procedure to
      -- create next revision for the fcast_header record
      -- It rely on calling procedure to perform basic range checking

      RETURN NUMBER
   IS
      l_revision             NUMBER;
      l_fcast_id_not_exist   EXCEPTION;
      PRAGMA EXCEPTION_INIT (l_fcast_id_not_exist, -20001);
   BEGIN
      -- throw error if fcast_id record does not exist at all

      SELECT MAX (revision)
        INTO l_revision
        FROM fcast_header
       WHERE fcast_id = p_fcast_id;

      IF l_revision IS NULL
      THEN
         RAISE l_fcast_id_not_exist;
      END IF;


      INSERT INTO fcast_header (FCAST_ID,
                                REVISION,
                                NAME,
                                FCAST_CATEGORY,
                                FCAST_TYPE,
                                DURATION,
                                STATE,
                                LR,
                                FCAST_BASE,
                                NETS_UPLOAD,
                                MTM_UPLOAD,
                                EXPOSURE_UPLOAD,
                                COMMENTS,
                                LASTCHANGED)
         SELECT p_fcast_id,
                l_revision + 1,
                name,
                FCAST_CATEGORY,
                FCAST_TYPE,
                DURATION,
                STATE,
                LR,
                FCAST_BASE,
                'N',
                'N',
                'N',
                p_comment,
                SYSDATE
           FROM fcast_header fh
          WHERE fh.fcast_id = p_fcast_id AND revision = l_revision;

      --   log_message ('create new revision for (fcast_id,rev)['||P_FCAST_ID || ',' || V_REVISION+1 ||']');
      RETURN l_revision + 1;
   EXCEPTION
      WHEN l_fcast_id_not_exist
      THEN
         raise_application_error (
            -20003,
            'fcast_id does not exist [' || p_fcast_id || ']',
            TRUE);
      WHEN OTHERS
      THEN
         --  log_message (err_stack);
         RAISE;
   END;



   PROCEDURE P_SAVE_FCAST (p_fcast_id     IN NUMBER,
                           p_revision     IN NUMBER,
                           p_expect_rec   IN NUMBER)
   IS
      v_name                   fcast_header.NAME%TYPE;
      v_count                  NUMBER (6);
      NO_RECORD                EXCEPTION;
      NOT_MATCH_EXPECTED_REC   EXCEPTION;
      PRAGMA EXCEPTION_INIT (NO_RECORD, -955);
      PRAGMA EXCEPTION_INIT (NOT_MATCH_EXPECTED_REC, -956);
   BEGIN
      INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                   REVISION,
                                   DATETIME,
                                   QUANTITY_MW,
                                   RATE)
         SELECT p_FCAST_ID,
                p_REVISION,
                DATETIME,
                quantity_mw,
                NVL (RATE, 0)
           FROM ppa_detail_temp;

      DBMS_OUTPUT.put_line ('rows inserted ' || SQL%ROWCOUNT);

      UPDATE TRADINGANALYSIS.FCAST_HEADER fh
         SET (fh.START_DATE, fh.FINISH_DATE) =
                (SELECT TRUNC (MIN (datetime)), TRUNC (MAX (datetime))
                   FROM ppa_detail_temp)
       WHERE fcast_id = p_fcast_id AND revision = P_REVISION;
   END;



   PROCEDURE P_LOAD_FCAST (p_scenario IN VARCHAR2, p_comment IN VARCHAR2)
   IS
      v_min   DATE;
      v_max   DATE;
   BEGIN
      SELECT TRUNC (MIN (datetime)), TRUNC (MAX (datetime))
        INTO v_min, v_max
        FROM temp_TAO;

      FOR REC IN (SELECT *
                    FROM fcast_header
                   WHERE mtm_upload = 'Y' AND fcast_type = P_SCENARIO)
      LOOP
         DBMS_OUTPUT.put_line (
            'fcast_id:' || rec.fcast_id || ' rev:' || rec.revision);

         INSERT INTO FCAST_HEADER (FCAST_ID,
                                   REVISION,
                                   NAME,
                                   FCAST_CATEGORY,
                                   FCAST_TYPE,
                                   DURATION,
                                   STATE,
                                   LR,
                                   FCAST_BASE,
                                   START_DATE,
                                   FINISH_DATE,
                                   NETS_UPLOAD,
                                   MTM_UPLOAD,
                                   EXPOSURE_UPLOAD,
                                   NETS_CONTRACT_ID,
                                   COMMENTS,
                                   LASTCHANGED)
              VALUES (REC.FCAST_ID,
                      REC.REVISION + 1,
                      REC.NAME,
                      REC.FCAST_CATEGORY,
                      REC.FCAST_TYPE,
                      REC.DURATION,
                      REC.STATE,
                      REC.LR,
                      REC.FCAST_BASE,
                      v_min,
                      v_max,
                      REC.NETS_UPLOAD,
                      'N',
                      'N',
                      rec.NETS_CONTRACT_ID,
                      p_comment,
                      SYSDATE);

         IF rec.state = 'NSW'
         THEN
            INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                         REVISION,
                                         DATETIME,
                                         QUANTITY_MW,
                                         RATE)
               SELECT REC.FCAST_ID,
                      REC.REVISION + 1,
                      DATETIME,
                      nsw_mw,
                      0
                 FROM temp_tao;

            DBMS_OUTPUT.put_line ('NSW rows inserted ' || SQL%ROWCOUNT);
         END IF;

         IF rec.state = 'QLD'
         THEN
            INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                         REVISION,
                                         DATETIME,
                                         QUANTITY_MW,
                                         RATE)
               SELECT REC.FCAST_ID,
                      REC.REVISION + 1,
                      DATETIME,
                      qld_mw,
                      0
                 FROM temp_tao;

            DBMS_OUTPUT.put_line ('QLD rows inserted ' || SQL%ROWCOUNT);
         END IF;

         IF rec.state = 'SA'
         THEN
            INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                         REVISION,
                                         DATETIME,
                                         QUANTITY_MW,
                                         RATE)
               SELECT REC.FCAST_ID,
                      REC.REVISION + 1,
                      DATETIME,
                      sa_mw,
                      0
                 FROM temp_tao;

            DBMS_OUTPUT.put_line ('SA rows inserted ' || SQL%ROWCOUNT);
         END IF;

         IF rec.state = 'VIC'
         THEN
            INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                         REVISION,
                                         DATETIME,
                                         QUANTITY_MW,
                                         RATE)
               SELECT REC.FCAST_ID,
                      REC.REVISION + 1,
                      DATETIME,
                      vic_mw,
                      0
                 FROM temp_tao;

            DBMS_OUTPUT.put_line ('VIC rows inserted ' || SQL%ROWCOUNT);
         END IF;
      END LOOP;
   END;

   PROCEDURE p_create_ppa_master (p_name            IN VARCHAR2,
                                  state             IN VARCHAR2 DEFAULT NULL,
                                  FUEL_TYPE         IN VARCHAR2 DEFAULT NULL,
                                  PORTFOLIO         IN VARCHAR2 DEFAULT NULL,
                                  COMMODITY         IN VARCHAR2 DEFAULT NULL,
                                  SUPPLY_CATEGORY   IN VARCHAR2 DEFAULT NULL,
                                  COUNTERPARTY      IN VARCHAR2 DEFAULT NULL,
                                  DUID              IN VARCHAR2 DEFAULT NULL,
                                  OE_ENTITY         IN VARCHAR2 DEFAULT NULL,
                                  cAPACITY_MW       IN VARCHAR2 DEFAULT NULL,
                                  CAPACITY_PERC     IN VARCHAR2 DEFAULT NULL,
                                  contract_desc     IN VARCHAR2 DEFAULT NULL)
   IS
      name_is_require_error   EXCEPTION;
      PRAGMA EXCEPTION_INIT (name_is_require_error, -20001);
      v_ppa_id                ppa_master.ppa_id%TYPE;
   BEGIN
      IF LENGTH (p_name) = 0
      THEN
         RAISE name_is_require_error;
      END IF;

      SELECT MAX (ppa_id) INTO v_ppa_id FROM ppa_master;

      INSERT INTO ppa_master (PPA_ID,
                              NAME,
                              FUEL_TYPE,
                              PORTFOLIO,
                              COMMODITY,
                              SUPPLY_CATEGORY,
                              COUNTERPARTY,
                              DUID,
                              STATE,
                              OE_ENTITY,
                              CAPACITY_MW,
                              CAPACITY_PERC,
                              contract_desc)
           VALUES (v_ppa_id + 1,
                   UPPER (p_name),
                   FUEL_TYPE,
                   PORTFOLIO,
                   COMMODITY,
                   SUPPLY_CATEGORY,
                   COUNTERPARTY,
                   DUID,
                   STATE,
                   OE_ENTITY,
                   CAPACITY_MW,
                   NVL (CAPACITY_PERC, 100),
                   contract_desc);
   EXCEPTION
      WHEN name_is_require_error
      THEN
         raise_application_error (
            -20001,
            'Error:  [' || p_name || ']' || ' cannot be blank',
            TRUE);
   END;

   --
   --FUNCTION get_day_type(day date,holiday varchar2) RETURN varchar2
   --IS
   --  day_type varchar2(3);
   --BEGIN
   --
   --  select
   --                           CAST(DECODE(TO_CHAR(trunc(DAY), 'DY'), 'SUN', 'NON',
   --                                                             'SAT', 'SAT',
   --           DECODE((SELECT COUNT(*) FROM CONTRACTS.HOLIDAYS k WHERE k.HOLIDAY_NAME = holiday AND k.HOLIDAY_DATE = trunc(DAY) AND ROWNUM = 1), 0, 'WOR', 'NON')) AS VARCHAR2(3)) DAY_CODE into day_type
   --           from dual;
   --
   --
   --   RETURN day_type;
   --END;
   --
   --
   --FUNCTION get_period_type(day date) RETURN varchar2
   --IS
   --  day_type varchar2(3);
   --BEGIN
   --
   --  select CAST(TO_CHAR(trunc(DAY), 'MON') AS VARCHAR2(3)) into day_type from dual;
   --
   --
   --   RETURN day_type;
   --END;


   PROCEDURE create_fcast_data (p_ppa_name         IN VARCHAR2,
                                p_start_date       IN DATE,
                                p_finish_date      IN DATE,
                                p_scaling          IN NUMBER DEFAULT 1,
                                p_ref_start_date   IN DATE)
   IS
      l_holiday   CONTRACTS.HOLIDAYS.HOLIDAY_NAME%TYPE;
      l_state     VARCHAR2 (3);
   /*
   p_ppa_name is the name of the ppa_station to create forecast data for.
   p_start_date is the start_date of the forecast
   p_finish_date is the finish_date of the forecast
   p_scaling is the number multiplier after the average is calculated.
   p_ref_start_date is the start date of the data to be extrapolated.
    */
   BEGIN
      BEGIN
         SELECT DISTINCT holidays
           INTO l_holiday
           FROM tx.site_table
          WHERE nmi =
                   (SELECT DISTINCT b.nmi_10
                      FROM ppa_master a
                           JOIN ppa_mapping b ON a.ppa_id = b.ppa_id
                     WHERE a.name = p_ppa_name AND ROWNUM <= 1);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            SELECT state
              INTO l_state
              FROM ppa_master
             WHERE name = p_ppa_name;

            CASE l_state
               WHEN 'NSW'
               THEN
                  l_holiday := 'NSWGazettedHolidays';
               WHEN 'QLD'
               THEN
                  l_holiday := 'QLDGazettedHolidays';
               WHEN 'VIC'
               THEN
                  l_holiday := 'VICGazettedHolidays';
               ELSE
                  l_holiday := 'SA-NEMHoliday';
            END CASE;
      END;

      DELETE TRADINGANALYSIS.PPA_DETAIL_TEMP;



      INSERT INTO PPA_DETAIL_TEMP (DATETIME, QUANTITY_MW)
         WITH days
              AS (SELECT get_day,
                         get_period_code (get_day) period_code,
                         get_day_type (get_day, l_holiday) day_code
                    FROM (    SELECT TRUNC (p_start_date) + LEVEL - 1 get_day
                                FROM DUAL
                          CONNECT BY LEVEL <=
                                          1
                                        + (  p_finish_date
                                           - TRUNC (p_start_date)))),
              AVG_DATA
              AS (  SELECT TO_CHAR (day, 'HH24:MI') dt,
                           day_code,
                           AVG (demand) * p_scaling QTY_MW
                      FROM (SELECT day,
                                   get_day_type (m.day, l_holiday) day_code,
                                   m.DEMAND
                              FROM (  SELECT mvm.datetime - 1 / 48 day,
                                             SUM (b_energy) / 1000 * 2 AS demand
                                        FROM metering.v_meterdata mvm
                                             JOIN
                                             tradinganalysis.v_ipa_ppa_mapping b
                                                ON (   b.nmi = mvm.nmi10
                                                    OR b.nmi = mvm.nmi)
                                       WHERE     b.name = P_ppa_name
                                             AND mvm.day >
                                                    GREATEST (p_ref_start_date,
                                                              '01-jan-2014')
                                    GROUP BY mvm.datetime) m)
                  GROUP BY TO_CHAR (day, 'HH24:MI'), day_code)
         SELECT TO_DATE (A.get_day || ' ' || B.DT, 'DD-MON-YY HH24:MI'),
                QTY_MW
           FROM DAYS A JOIN AVG_DATA B ON a.day_code = b.day_code;

      UPDATE PPA_DETAIL_TEMP
         SET DATETIME = p_finish_date + 1
       WHERE DATETIME = p_start_date;

      COMMIT;
   END;



   PROCEDURE refresh_ppa_hh_tni_ACTUAL(p_last_run_date in date default null)
   IS
      v_last_run   DATE;
      v_days_ago number(3):=5;
   BEGIN
      
      BEGIN
         SELECT (NVL (D_value, TRUNC (SYSDATE)) - v_days_ago)
           INTO v_last_run
           FROM TP_PARAMETERS
          WHERE NAME = 'PPA_ACTUAL_LAST_SNAPSHOT';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            v_last_run := TRUNC (SYSDATE) - v_days_ago;
      END;
      
      v_last_run:=nvl(p_last_run_date,v_last_run);

      execute immediate 'truncate table tp_PPA_TNI_HH_TEMP';

      INSERT INTO tp_PPA_TNI_HH_TEMP (NAME,
                                      DATETIME,
                                      QUANTITY_MW,
                                      QUALITY,
                                      STATE,
                                      REGIONID,
                                      DATA_QUALITY,
                                      TNI)
         WITH 
              smith
              AS (                                       -- GET PPA_SMITHFIELD
                  SELECT   ff datetime, AVG (scadavalue) quantity_mw
                      FROM (SELECT p.NAME,
                                   p.regionid,
                                   p.state,
                                   sc.scadavalue,
                                   CASE
                                      WHEN TO_NUMBER (
                                              TO_CHAR (settlementdate, 'mi')) BETWEEN 5
                                                                                  AND 30
                                      THEN
                                           TRUNC (settlementdate, 'hh24')
                                         + 1 / 48
                                      WHEN TO_NUMBER (
                                              TO_CHAR (settlementdate, 'mi')) =
                                              0
                                      THEN
                                         TRUNC (settlementdate, 'hh24')
                                      ELSE
                                           TRUNC (settlementdate, 'hh24')
                                         + 2 / 48
                                   END
                                      AS ff
                              FROM TRADINGANALYSIS.V_TP_PPA_TNI p
                                   JOIN mms.dispatch_unit_scada sc
                                      ON p.duid = sc.duid
                             WHERE     sc.settlementdate >=
                                          v_last_run
                                   AND sc.settlementdate >= p.start_date
                                   AND sc.settlementdate <= p.finish_date
                                   AND p.NAME = 'PPA_SMITHFIELD')
                  GROUP BY NAME,
                           regionid,
                           state,
                           ff),
              emb
              AS (                                  -- GET EMBEDDED GENERATION
                  SELECT i.gen_date + period / 48 datetime, i.mw quantity_mw
                    FROM staging.v_ie_hh_emb_gen_data i
                   WHERE     i.gen_date >=v_last_run
                         AND i.nmi = 'EMBEDEDGEN'),
              appin_ppa
              AS (SELECT 'PPA_APPIN' NAME,
                         b.datetime,
                         e.quantity_mw - b.quantity_mw quantity_mw,
                         'ACTUAL' quality,
                         m.state,
                         m.regionid,
                         m.tni_conf tni
                    FROM smith b, emb e, TRADINGANALYSIS.V_TP_PPA_TNI m
                   WHERE     m.NAME = 'PPA_APPIN'
                         AND b.datetime = e.datetime
                         AND b.datetime >=
                               v_last_run
                         AND b.datetime >= m.start_datetime
                         AND b.datetime <= m.finish_datetime),
              md
              AS (  SELECT b.NAME,
                           mvm.datetime,
                           SUM (b_energy) / 1000 * 2 AS quantity_mw,
                           'ACTUAL' quality,
                           b.state,
                           b.regionid,
                           b.tni_conf tni
                      FROM metering.v_meterdata mvm
                           JOIN TRADINGANALYSIS.V_TP_PPA_TNI b
                              ON (b.nmi = mvm.nmi10 OR b.nmi = mvm.nmi)
                     WHERE     mvm.DAY >= v_last_run
                           AND mvm.day >= b.start_date
                           AND mvm.day <= b.finish_date
                  GROUP BY b.NAME,
                           mvm.datetime,
                           b.state,
                           b.regionid,
                           b.tni_conf),
              ce_met
              AS (-- GET CE PPA
                   select name,
                    datetime,
                    quantity_mw,
                    quality,
                    state,
                    regionid,
                    tni from (SELECT b.NAME,
                         i.datetime,
                         i.mw quantity_mw,
                         'ACTUAL' quality,
                         b.state,
                         b.regionid,
                         b.tni_conf tni,
                         row_number() over (partition by name,datetime,mw order by i.file_id desc) rn
                    FROM staging.v_consumptn_nem12_ppa_latest i
                         JOIN TRADINGANALYSIS.V_TP_PPA_TNI b
                            ON i.nmi = b.nmi
                   WHERE      
                         i.nmi_suffix = 'B1'
                         AND i.datetime >= b.start_datetime
                         AND i.datetime <= b.finish_datetime) where rn=1),
              scada
              AS (  SELECT NAME,
                           datetime,
                           SUM (quantity_mw) quantity_mw,
                           quality,
                           state,
                           regionid,
                           tni_conf tni
                      FROM (  SELECT aa.NAME,
                                     ff datetime,
                                     ROUND (AVG (aa.scadavalue), 2) quantity_mw,
                                     'ACTUAL' quality,
                                     aa.state,
                                     aa.regionid,
                                     aa.duid,
                                     bb.tni_conf
                                FROM (SELECT p.NAME,
                                             p.regionid,
                                             p.state,
                                             sc.scadavalue,
                                             CASE
                                                WHEN TO_NUMBER (
                                                        TO_CHAR (settlementdate,
                                                                 'mi')) BETWEEN 5
                                                                            AND 30
                                                THEN
                                                     TRUNC (settlementdate,
                                                            'hh24')
                                                   + 1 / 48
                                                WHEN TO_NUMBER (
                                                        TO_CHAR (settlementdate,
                                                                 'mi')) = 0
                                                THEN
                                                   TRUNC (settlementdate, 'hh24')
                                                ELSE
                                                     TRUNC (settlementdate,
                                                            'hh24')
                                                   + 2 / 48
                                             END
                                                AS ff,
                                             p.duid
                                        FROM TRADINGANALYSIS.V_TP_PPA_TNI p
                                             JOIN mms.dispatch_unit_scada sc
                                                ON     p.duid = sc.duid
                                                   AND sc.settlementdate >=
                                                        v_last_run
                                                   AND sc.settlementdate >=
                                                          p.start_date
                                                   AND sc.settlementdate <=
                                                          p.finish_date) aa
                                     JOIN TRADINGANALYSIS.V_TP_PPA_TNI bb
                                        ON aa.name = bb.name
                            GROUP BY aa.NAME,
                                     aa.regionid,
                                     aa.state,
                                     ff,
                                     aa.duid,
                                     bb.tni_conf)
                  GROUP BY NAME,
                           datetime,
                           quality,
                           state,
                           regionid,
                           tni_conf)
         SELECT *
           FROM (SELECT a.name,
                        a.datetime,
                        a.quantity_mw,
                        a.quality,
                        a.state,
                        a.regionid,
                        1 data_quality,
                        a.tni
                   FROM appin_ppa a
                 UNION ALL
                 SELECT a.name,
                        a.datetime,
                        a.quantity_mw,
                        a.quality,
                        a.state,
                        a.regionid,
                        1 data_quality,
                        a.tni
                   FROM scada a
                 UNION ALL
                 SELECT a.name,
                        a.datetime,
                        a.quantity_mw,
                        a.quality,
                        a.state,
                        a.regionid,
                        2 data_quality,
                        a.tni
                   FROM md a
                   union all
                   SELECT a.name,
                        a.datetime,
                        a.quantity_mw,
                        a.quality,
                        a.state,
                        a.regionid,
                        3 data_quality,
                        a.tni
                   FROM ce_met a
                   );
                   
                   
                   
       pkg_file_loader.LOG_MESSAGE('row inserted:'||sql%rowcount);
       
       COMMIT;

      MERGE INTO TP_PPA_HH_TNI_ACTUAL d
           USING (SELECT a.name,
                         a.datetime,
                         quantity_mw,
                         a.quality,
                         a.state,
                         a.regionid,
                         a.data_quality,
                         a.tni
                    FROM tp_PPA_TNI_HH_TEMP a
                        ) s
              ON (    d.name = s.name
                  AND d.datetime = s.datetime
                  AND d.tni = s.tni
                  AND d.state = s.state
                  and d.data_quality = s.data_quality )
      when not matched then
       INSERT (d.name,
                         d.datetime,
                         d.quantity_mw,
                         d.quality,
                         d.state,
                         d.regionid,
                         d.data_quality,
                         d.tni)
        values(s.name,
                         s.datetime,
                         s.quantity_mw,
                         s.quality,
                         s.state,
                         s.regionid,
                         s.data_quality,
                         s.tni)
      WHEN MATCHED
      THEN
         UPDATE SET
            d.quantity_mw = s.quantity_mw,
            d.quality = s.quality;
      pkg_file_loader.LOG_MESSAGE('row merged:'||sql%rowcount);
      
      update  TP_PARAMETERS set D_value=sysdate
          WHERE NAME = 'PPA_ACTUAL_LAST_SNAPSHOT';

      COMMIT;
   END;
   
   PROCEDURE refresh_ppa_hh_tni_forecast(p_last_run_date in date default null)
   IS
      v_last_run   DATE;
      v_days_ago number(3):=5;
   BEGIN
      
      BEGIN
         SELECT (NVL (D_value, TRUNC (SYSDATE)) - v_days_ago)
           INTO v_last_run
           FROM TP_PARAMETERS
          WHERE NAME = 'PPA_FORCAST_LAST_SNAPSHOT';
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            v_last_run := TRUNC (SYSDATE) - v_days_ago;
      END;
      
      v_last_run:=nvl(p_last_run_date,v_last_run);

      INSERT INTO TP_PPA_TNI_HH_TMP (NAME,
                                      DATETIME,
                                      QUANTITY_MW,
                                      QUALITY,
                                      STATE,
                                      REGIONID,
                                      DATA_QUALITY,
                                      TNI)
          SELECT NAME,
       datetime,
       quantity_mw,
       'FORECAST' quality,
       state,
       regionid,
       6 data_quality,
       tni
  FROM (SELECT h.NAME,
               d.datetime,
               h.revision,
               ipa.state,
               ipa.regionid,
               d.quantity_mw,
               ROW_NUMBER ()
               OVER (PARTITION BY h.NAME, d.datetime
                     ORDER BY h.revision DESC)
                  rn,
                  ipa.tni_conf tni
          FROM tradinganalysis.fcast_header h
               LEFT JOIN TRADINGANALYSIS.V_TP_PPA_TNI ipa
                  ON h.NAME = ipa.NAME
               JOIN tradinganalysis.fcast_detail_hh d
                  ON h.fcast_id = d.fcast_id AND h.revision = d.revision
         WHERE     h.fcast_category = 'PPA'
               AND DURATION = 'LONG_TERM'
               and D.DATETIME >= IPA.START_DATETIME and d.datetime <= IPA.FINISH_DATETIME
               and d.datetime >= v_last_run
)
 WHERE rn = 1;
                   
                   
                   
       pkg_file_loader.LOG_MESSAGE('row inserted:'||sql%rowcount);
       
     

      MERGE INTO TP_PPA_HH_TNI_FORECAST d
           USING (SELECT a.name,
                         a.datetime,
                         quantity_mw,
                         a.quality,
                         a.state,
                         a.regionid,
                         a.data_quality,
                         a.tni
                    FROM TP_PPA_TNI_HH_TMP a
                        ) s
              ON (    d.name = s.name
                  AND d.datetime = s.datetime
                  AND d.tni = s.tni
                  AND d.state = s.state
                  and d.data_quality = s.data_quality )
      when not matched then
       INSERT (d.name,
                         d.datetime,
                         d.quantity_mw,
                         d.quality,
                         d.state,
                         d.regionid,
                         d.data_quality,
                         d.tni)
        values(s.name,
                         s.datetime,
                         s.quantity_mw,
                         s.quality,
                         s.state,
                         s.regionid,
                         s.data_quality,
                         s.tni)
      WHEN MATCHED
      THEN
         UPDATE SET
            d.quantity_mw = s.quantity_mw,
            d.quality = s.quality;
      pkg_file_loader.LOG_MESSAGE('row merged:'||sql%rowcount);
      
      update  TP_PARAMETERS set D_value=sysdate
          WHERE NAME = 'PPA_FORECAST_LAST_SNAPSHOT';

      COMMIT;
   END;
   
END;
/