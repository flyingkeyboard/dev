CREATE OR REPLACE PACKAGE BODY TRADINGANALYSIS.pkg_fcast_maint
AS
   /******************************************************************************
      NAME:       pkg_fcast_maint
      PURPOSE:

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        02/02/2015      taop       1. Created this package.
   ******************************************************************************/



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



   PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;

      l_user_name   VARCHAR2 (60);
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'OS_USER') INTO l_user_name FROM DUAL;

      INSERT INTO tradinganalysis.message_log (TIME, MESSAGE, USERID)
              VALUES (
                        SYSDATE,
                        p_message || ':' || seq_file_id.NEXTVAL,
                        l_user_name);

      COMMIT;

      FOR i IN 1 .. 10000
      LOOP
         NULL;
      END LOOP;
   END;



   PROCEDURE IMPORT_DATA (p_file_id    IN NUMBER,
                          p_fcast_id   IN NUMBER,
                          p_rev        IN NUMBER)
   IS
      l_rows              NUMBER (10);
      l_no_row_loaded     EXCEPTION;
      l_load_row_failed   EXCEPTION;
      PRAGMA EXCEPTION_INIT (l_no_row_loaded, -20002);
   BEGIN
      SELECT COUNT (*) INTO l_rows FROM gl_staging_volume;


      INSERT INTO FCAST_DETAIL_HH (FCAST_ID,
                                   REVISION,
                                   DATETIME,
                                   QUANTITY_MW,
                                   rate)
         SELECT p_fcast_id,
                p_rev,
                datetime,
                quantity_mw,
                rate
           FROM gl_staging_volume;


      IF l_rows = 0
      THEN
         log_message (
            'No row in gl_load_volume.  Maybe the csv file has no datetime,mw heading?');
         RAISE l_no_row_loaded;
      ELSIF SQL%ROWCOUNT <> l_rows
      THEN
         RAISE l_load_row_failed;
      END IF;
   EXCEPTION
      WHEN l_no_row_loaded
      THEN
         raise_application_error (
            -20002,
               '!No row is loaded! ['
            || l_rows
            || '] maybe the csv file has no datetime,mw heading?',
            TRUE);
      WHEN l_load_row_failed
      THEN
         raise_application_error (-20004, '!Some row did not load![', TRUE);
   END;

   PROCEDURE PREPARE_LOAD (p_file_id    IN NUMBER,
                           p_fcast_id   IN NUMBER,
                           p_rev        IN NUMBER)
   IS
   BEGIN
      log_message ('prepare load');

      DELETE gl_staging_volume;

      INSERT INTO gl_staging_volume (DATETIME, QUANTITY_MW, rate)
         SELECT datetime, quantity_mw, rate FROM v_gl_load_volume;
   END;


   PROCEDURE PREPARE_STATE_LOAD (p_file_id    IN NUMBER,
                                 p_fcast_id   IN NUMBER,
                                 p_rev        IN NUMBER)
   IS
      v_start_date        DATE;
      v_finish_date       DATE;
      v_state             VARCHAR2 (10);
      l_rows              NUMBER (10);
      l_no_row_loaded     EXCEPTION;
      l_load_row_failed   EXCEPTION;
      PRAGMA EXCEPTION_INIT (l_no_row_loaded, -20002);
   BEGIN
      DELETE gl_staging_volume;

      BEGIN
         SELECT REPLACE (state, GC_OVR_PREFIX, '')
           INTO v_state
           FROM fcast_header
          WHERE fcast_id = p_fcast_id AND revision = p_rev;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            raise_application_error (
               -20004,
                  'cannot find the fcast_header record. fcast_id['
               || p_fcast_id
               || '],rev['
               || p_rev
               || ']',
               TRUE);
      END;

      log_message ('prepare state load for ' || v_state);

      SELECT TRUNC (MIN (datetime)), MAX (datetime)
        INTO v_start_date, v_finish_date
        FROM v_gl_load_volume;

      -- get the retail CI load so that it can be used to get MM volume = state_load - CI

--      INSERT INTO gl_staging_volume (DATETIME, QUANTITY_MW)
--         WITH cc
--              AS (SELECT contract_id,
--                         rev,
--                         REPLACE (REPLACE (contract_group, '-RETAIL', ''),
--                                  '-WHOLESALE',
--                                  '')
--                            state
--                    FROM contracts.contracts c
--                         LEFT OUTER JOIN settle.counterparty p
--                            ON p.counterparty_id = c.counterparty
--                   WHERE     c.contract_type = 'RETAIL_SUMMARY'
--                         AND c.built = 1
--                         AND c.verified = 1
--                         AND c.status = 'OK'
--                         AND c.finish_date >= v_start_date
--                         AND c.start_date <= v_finish_date),
--              ci_vol
--              AS (  SELECT hh.datetime,
--                           SUM (
--                              (  DECODE (hh.option_quantity,
--                                         NULL, hh.quantity,
--                                         0, hh.quantity,
--                                         hh.option_quantity)
--                               * hh.direction))
--                              AS ci_mw
--                      FROM tx.hh_transactions hh
--                           JOIN cc l
--                              ON     hh.contract_id = l.contract_id
--                                 AND hh.rev = l.rev
--                           JOIN v_gl_load_volume gl
--                              ON hh.datetime = gl.datetime
--                     WHERE     hh.contract_type = 23
--                           AND hh.ttype = 'C'
--                           AND l.state = hh.state
--                           AND l.state = v_state
--                  GROUP BY hh.datetime,
--                           hh.option_quantity,
--                           hh.option_quantity)
--         SELECT a.datetime, gl.quantity_mw - ABS (a.ci_mw) vol_mw
--           FROM ci_vol a JOIN v_gl_load_volume gl ON a.datetime = gl.datetime;

      l_rows := SQL%ROWCOUNT;

      log_message ('loaded rows:[' || l_rows || ']');

      IF l_rows = 0
      THEN
         raise_application_error (
            -20004,
               'error cannot find retail CI records fcast_id['
            || p_fcast_id
            || '],rev['
            || p_rev
            || ']',
            TRUE);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (
            -20010,
               'error: cannot insert retail CI records fcast_id['
            || p_fcast_id
            || '],rev['
            || p_rev
            || '] '
            || err_stack,
            TRUE);
   END;


   PROCEDURE validate_import_data (p_file_id IN NUMBER, p_fcast_id IN NUMBER)
   IS
      l_data_range_mismatch   EXCEPTION;
      PRAGMA EXCEPTION_INIT (l_data_range_mismatch, -20005);
      l_mindate_s             DATE;
      l_maxdate_s             DATE;
      l_count                 NUMBER (2);
   BEGIN
      -- need a procedure to validate if the data date range matches a given fcast_id
      -- it will return an error
      SELECT MIN (datetime), MAX (datetime)
        INTO l_mindate_s, l_maxdate_s
        FROM TRADINGANALYSIS.v_gl_LOAD_VOLUME s;

      --  WHERE s.file_id = p_file_id;

      --      v_mindate_s:= '30-jan-2015';
      --      v_maxdate_s:='04-feb-2015';

      SELECT COUNT (*)
        INTO l_count
        FROM fcast_header fh
       WHERE     fh.fcast_id = p_fcast_id
             AND revision = (SELECT MAX (revision)
                               FROM fcast_header fh
                              WHERE fh.fcast_id = p_fcast_id)
             AND start_date <= l_mindate_s
             AND finish_date >= l_maxdate_s;

      IF l_count = 0
      THEN
         RAISE l_data_range_mismatch;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (
            -20005,
               'import data range does not fall within fcast_id['
            || p_fcast_id
            || '] start_date['
            || l_mindate_s
            || '] finish_date ['
            || l_maxdate_s
            || ']',
            TRUE);
   END;

   FUNCTION get_revision (p_fcast_id IN NUMBER)
      RETURN NUMBER
   IS
      l_REV   FCAST_HEADER.REVISION%TYPE;
   BEGIN
      BEGIN
         SELECT MAX (REVISION)
           INTO l_REV
           FROM FCAST_HEADER FH
          WHERE FH.FCAST_ID = P_FCAST_ID;

         RETURN l_REV;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_REV := NULL;
            RETURN l_REV;
      END;
   END;

   PROCEDURE get_fcast_id (p_fcast_category   IN     VARCHAR2,
                           p_duration         IN     VARCHAR2,
                           p_state            IN     VARCHAR2,
                           p_fcastid             OUT NUMBER)
   IS
   BEGIN
      p_fcastid := get_fcast_id (p_fcast_category, p_duration, p_state);
   END;


   PROCEDURE get_mm_fcast_header (p_duration         IN     VARCHAR2,
                                  p_state            IN     VARCHAR2,
                                  out_load_request      OUT SYS_REFCURSOR)
   IS
   -- to do for loading mm forecast
   BEGIN
      NULL;
   END;

   PROCEDURE get_fcast_header (p_fcast_category   IN     VARCHAR2,
                               p_duration         IN     VARCHAR2,
                               p_state            IN     VARCHAR2,
                               out_load_request      OUT SYS_REFCURSOR)
   IS
      l_fcast_id         fcast_header.fcast_id%TYPE;
      l_fcast_category   fcast_header.fcast_category%TYPE;
      l_duration         fcast_header.duration%TYPE;
      l_state            fcast_header.state%TYPE;
   BEGIN
      l_fcast_category := GC_OVR_PREFIX || p_fcast_category;
      l_duration := GC_OVR_PREFIX || p_duration;
      l_state := GC_OVR_PREFIX || p_state;
      l_fcast_id := get_fcast_id (l_fcast_category, l_duration, l_state);

      OPEN out_load_request FOR
         SELECT fcast_id,
                REPLACE (name, 'OVR_', '') NAME,
                REPLACE (fcast_category, 'OVR_', '') FCAST_CATEGORY,
                REPLACE (fcast_type, 'OVR_', '') FCAST_TYPE,
                REPLACE (duration, 'OVR_', '') DURATION,
                REPLACE (state, 'OVR_', '') STATE,
                REPLACE (lr, 'OVR_', '') LR,
                to_char((select max(trunc(start_date)) from TRADINGANALYSIS.FCAST_HEADER where fcast_id = fh.fcast_id-1000),'DD-MON-YYYY') start_date,
                TO_CHAR((select max(trunc(finish_date)) from TRADINGANALYSIS.FCAST_HEADER where fcast_id = fh.fcast_id-1000),'DD-MON-YYYY') finish_date,
                (select active from TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING m where m.fcast_id = fh.fcast_id) Active
           FROM TRADINGANALYSIS.FCAST_HEADER fh
          WHERE     fcast_id = l_fcast_id
                AND revision = (SELECT MAX (revision)
                                  FROM fcast_header f
                                 WHERE f.fcast_id = l_fcast_id);
   END;
   
   
    PROCEDURE get_fcast_headers (p_fcast_category   IN     VARCHAR2 default null,
                               p_duration         IN     VARCHAR2 default null,
                               p_state            IN     VARCHAR2 default null,
                               out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
      select
      fcast_id,
      name,
      fcast_category,
      fcast_type,
      duration,
      state,
      to_char((select max(trunc(start_date)) from TRADINGANALYSIS.FCAST_HEADER where fcast_id = m.fcast_id-1000),'DD-MON-YYYY') start_date,
      TO_CHAR((select max(trunc(finish_date)) from TRADINGANALYSIS.FCAST_HEADER where fcast_id = m.fcast_id-1000),'DD-MON-YYYY') finish_date,
      active        
      from   TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING m where m.data_source_id = 3
            and (M.FCAST_CATEGORY=p_fcast_category or p_fcast_category is null)
            and (M.STATE=p_state or p_state is null)
            and (m.duration = p_duration or p_duration is null);
      
  
   END;


   PROCEDURE get_fcast_override (p_fcast_id         IN     NUMBER,
                                 out_load_request      OUT SYS_REFCURSOR)
   IS
   l_start_date date;
   l_finish_date date;
   
   
   
   BEGIN
   
      if p_fcast_id > 3000 and p_fcast_id < 4000 then
      
        SELECT start_date,
                                 finish_date into
                                 l_start_date,
                                 l_finish_date  
                    FROM (SELECT fh.fcast_id,
                                 revision,
                                 start_date,
                                 finish_date,
                                 ROW_NUMBER ()
                                 OVER (
                                    PARTITION BY fcast_id
                                    ORDER BY revision DESC)
                                    rn
                            FROM TRADINGANALYSIS.FCAST_HEADER fh
                           WHERE fh.fcast_id = p_fcast_id-1000)
                   WHERE rn = 1;   
      
      end if;
   
      OPEN out_load_request FOR
       WITH src
              AS (SELECT fcast_id, revision
                    FROM (SELECT fh.fcast_id,
                                 revision,
                                 ROW_NUMBER ()
                                 OVER (
                                    PARTITION BY fcast_id,
                                                 start_date,
                                                 finish_date
                                    ORDER BY revision DESC)
                                    rn
                            FROM TRADINGANALYSIS.FCAST_HEADER fh
                           WHERE fh.fcast_id = p_fcast_id)
                   WHERE rn = 1)
           SELECT dd.fcast_id,
                  dd.revision,
                  MIN (datetime) min_date,
                  MAX (datetime) max_date,
                  TO_CHAR (SUM (quantity_mw), '9999999.99') mw,
                  (SELECT FH.LASTCHANGED
                     FROM fcast_header fh
                    WHERE     fh.fcast_id = dd.fcast_id
                          AND fh.revision = dd.revision)
                     last_change,
                     'datetime in range' status
             FROM fcast_detail_hh dd
                  JOIN src s
                     ON dd.fcast_id = s.fcast_id AND dd.revision = s.revision
                     where dd.datetime > l_start_date and dd.datetime <= l_start_date
         GROUP BY dd.fcast_id, dd.revision
         union all
         SELECT dd.fcast_id,
                  dd.revision,
                  MIN (datetime) min_date,
                  MAX (datetime) max_date,
                  TO_CHAR (SUM (quantity_mw), '9999999.99') mw,
                  (SELECT FH.LASTCHANGED
                     FROM fcast_header fh
                    WHERE     fh.fcast_id = dd.fcast_id
                          AND fh.revision = dd.revision)
                     last_change,
                     'ERROR: date not in range of '||l_start_date||','||l_finish_date status
             FROM fcast_detail_hh dd
                  JOIN src s
                     ON dd.fcast_id = s.fcast_id AND dd.revision = s.revision
                     where (dd.datetime < l_start_date or dd.datetime > l_start_date)
         GROUP BY dd.fcast_id, dd.revision;
   END;



   FUNCTION get_fcast_name (p_fcast_id IN NUMBER)
      RETURN VARCHAR2
   IS
      l_name   fcast_header.name%TYPE;
   BEGIN
      SELECT name
        INTO l_name
        FROM fcast_header
       WHERE fcast_id = p_fcast_id AND revision = 1;

      RETURN l_name;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         l_name := NULL;
         RETURN l_name;
   END;

   PROCEDURE get_fcast_name (p_fcast_id IN NUMBER, p_name OUT VARCHAR2)
   IS
   BEGIN
      p_name := get_fcast_name (p_fcast_id);
   END;



   FUNCTION get_fcast_id (p_fcast_category   IN VARCHAR2,
                          p_duration         IN VARCHAR2,
                          p_state            IN VARCHAR2)
      RETURN NUMBER
   IS
      l_fcast_id   NUMBER;
   BEGIN
      SELECT fcast_id
        INTO l_fcast_id
        FROM (SELECT fcast_id,
                     ROW_NUMBER ()
                        OVER (PARTITION BY fcast_id ORDER BY revision DESC)
                        rn
                FROM fcast_header fh
               WHERE     FH.FCAST_CATEGORY LIKE
                            '%' || p_fcast_category || '%'
                     AND FH.DURATION LIKE '%' || p_duration || '%'
                     AND FH.STATE LIKE '%' || p_state || '%'--AND fcast_id BETWEEN 3000 AND 3999
             )
       WHERE rn = 1;

      RETURN l_fcast_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (
            -20003,
               'cannot find fcast_id 3xxx range  ['
            || p_fcast_category
            || ']'
            || ',['
            || p_duration
            || ']'
            || ',['
            || p_state
            || ']',
            TRUE);
         RETURN NULL;
   END;



   PROCEDURE get_fcast_name (p_fcast_category   IN     VARCHAR2,
                             p_duration         IN     VARCHAR2,
                             p_state            IN     VARCHAR2,
                             p_name                OUT VARCHAR2,
                             p_fcastid             OUT NUMBER)
   IS
   BEGIN
      p_fcastid := get_fcast_id (p_fcast_category, p_duration, p_state);
      p_name := get_fcast_name (p_fcastid);
   END;
   
    PROCEDURE get_fcast_names2 (p_fcast_category   IN     VARCHAR2,
                               p_duration         IN     VARCHAR2,
                               p_state            IN     VARCHAR2,
                               p_fcast_type       IN     VARCHAR2,
                               out_load_request      OUT SYS_REFCURSOR)
    is
    begin
    
    get_fcast_names2 (p_fcast_category,
                               p_duration,
                               p_state,
                               p_fcast_type,
                               null,
                               out_load_request);
    end;



   PROCEDURE get_fcast_names2 (p_fcast_category   IN     VARCHAR2,
                               p_duration         IN     VARCHAR2,
                               p_state            IN     VARCHAR2,
                               p_fcast_type       IN     VARCHAR2,
                               p_official         IN     VARCHAR2,
                               out_load_request      OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
           SELECT FCAST_ID,
                  REVISION,
                  NAME,
                  START_DATE,
                  FINISH_DATE,
                  FCAST_CATEGORY,
                  FCAST_TYPE,
                  DURATION,
                  STATE,
                  COMMENTS,
                  LASTCHANGED,
                  EXPOSURE_UPLOAD,
                  mtm_upload,
                  NETS_UPLOAD
             FROM (SELECT FCAST_ID,
                          REVISION,
                          NAME,
                          FCAST_CATEGORY,
                          FCAST_TYPE,
                          DURATION,
                          STATE,
                          TO_CHAR (START_DATE, 'DD/MM/YYYY') START_DATE,
                          TO_CHAR (FINISH_DATE, 'DD/MM/YYYY') FINISH_DATE,
                          FH.COMMENTS,
                          FH.LASTCHANGED,
                          FH.EXPOSURE_UPLOAD,
                          fh.mtm_upload,
                          FH.NETS_UPLOAD,
                          ROW_NUMBER ()
                          OVER (PARTITION BY fcast_id ORDER BY revision DESC)
                             rn
                     FROM TRADINGANALYSIS.FCAST_HEADER fh
                    WHERE     (duration = p_duration or p_duration is null)
                          AND (fcast_category = p_fcast_category or p_fcast_category is null)
                          AND (state = p_state OR p_state IS NULL)
                          AND (exposure_UPLOAD = p_official or p_official is null)
                          AND (fh.fcast_type = p_fcast_type or p_fcast_type is null)
                          and FH.LASTCHANGED > '01-jan-2014'
                          )
           WHERE rn = 1
         ORDER BY name;
   END;

  

   PROCEDURE get_fcast_override_ranking (out_load_request OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
         SELECT data_source_id,
                rank_order,
                data_source,
                active
           FROM TRADINGANALYSIS.GL_FCAST_OVERRIDE_RANK;
   END;

   PROCEDURE update_fcast_header (p_file_id    IN NUMBER,
                                  p_fcast_id   IN NUMBER,
                                  p_rev        IN NUMBER)
   IS
      l_comment   TRADINGANALYSIS.gl_FILE_PARAMS.val%TYPE;
   BEGIN
      l_comment := PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, gc_comments);

      UPDATE fcast_header
         SET (start_date, finish_date) =
                (SELECT TRUNC (MIN (datetime)), MAX (datetime)
                   FROM fcast_detail_hh
                  WHERE fcast_id = p_fcast_id AND revision = p_rev),
             comments = l_comment
       WHERE fcast_id = p_fcast_id AND revision = p_rev;
   --       pkg_file_loader.create_loader_param(p_file_id ,
   --                   gc_NEW_REVISION,
   --                   p_rev,
   --                   gc_VARCHAR2);

   END;

   PROCEDURE PPA_UPLOAD (p_file_id IN NUMBER)
   IS
      l_fcast_id   fcast_header.fcast_id%TYPE;
      l_rev        fcast_header.revision%TYPE;
      l_category   gl_file_type.category%TYPE;
      l_name       TRADINGANALYSIS.gl_FILE_PARAMS.val%TYPE;
   BEGIN
      log_message ('start data load file_id[' || p_file_id || ']');
      l_category :=
         PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, gc_category);
      l_fcast_id := PKG_FILE_LOADER.GET_VARCHAR2_PARAM (P_FILE_ID, gc_fcastid);
      get_fcast_name (l_fcast_id, l_name);
      create_ppa_revision (l_name, l_FCAST_ID, l_rev);
      log_message (
            'import data to fcast_id['
         || l_fcast_id
         || '],revision['
         || l_rev
         || ']');
      PREPARE_LOAD (p_file_id, l_fcast_id, l_rev);
      IMPORT_DATA (p_file_id, l_fcast_id, l_rev);
      update_fcast_header (p_file_id, l_fcast_id, l_rev);
      pkg_file_loader.process_complete (p_file_id);
      pkg_file_loader.create_loader_param (p_file_id,
                                           gc_NEW_REVISION,
                                           L_rev,
                                           gc_VARCHAR2);
      --   audit_log (p_file_id);
      -- refresh mviews
--      DBMS_SCHEDULER.run_job (
--         job_name              => 'TRADINGANALYSIS.REFRESH_IPA_PPA_FCAST',
--         use_current_session   => FALSE);
      log_message ('end data load file_id[' || p_file_id || ']');
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message ('file_id[' || p_file_id || '] error' || err_stack);
         pkg_file_loader.process_error (p_file_id, err_stack);
         ROLLBACK;
         RAISE;
   END;


   PROCEDURE OVERRIDE_LOAD (p_file_id IN NUMBER)
   IS
      l_fcast_id   fcast_header.fcast_id%TYPE;
      l_rev        fcast_header.revision%TYPE;
      l_category   gl_file_type.category%TYPE;
      l_param      TRADINGANALYSIS.gl_FILE_PARAMS.val%TYPE;
   BEGIN
      log_message (
         l_category || ':start data load file_id[' || p_file_id || ']');
      l_category :=
         PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, gc_category);

      IF l_category = GC_STATE_LOAD
      THEN
         BEGIN
            -- create fcast_header/detail record and load the input file value as it is
            l_fcast_id :=
               PKG_FILE_LOADER.GET_NUMBER_PARAM (P_FILE_ID,
                                                 GC_STATE_LOAD_FCASTID);
            l_rev := create_fcast_override (l_fcast_id);
            log_message ('A> get new revision' || l_rev);
            PREPARE_LOAD (p_file_id, l_fcast_id, l_rev);
            log_message ('A>before data import');
            IMPORT_DATA (p_file_id, l_fcast_id, l_rev);
            log_message ('A> completed load fcast_id:' || l_fcast_id);
            update_fcast_header (p_file_id, l_fcast_id, l_rev);
            pkg_file_loader.create_loader_param (p_file_id,
                                                 gc_STATE_LOAD_NEW_REVISION,
                                                 L_rev,
                                                 gc_VARCHAR2);
         -- split state_load into MM and CI
         EXCEPTION
            WHEN OTHERS
            THEN
               RAISE;
         END;

         -- create fcast_header/detail record and get the MM load by taking C&I from state load (input value)
         l_fcast_id :=
            PKG_FILE_LOADER.GET_NUMBER_PARAM (P_FILE_ID, GC_FCASTID);
         l_rev := create_fcast_override (l_fcast_id);
         log_message (
               'B> get new revision fcast_id['
            || l_fcast_id
            || ' ['
            || l_rev
            || ']');
         PREPARE_STATE_LOAD (p_file_id, l_fcast_id, l_rev);
         log_message ('B> before data import');
         IMPORT_DATA (p_file_id, l_fcast_id, l_rev);
         log_message ('B> completed load fcast_id:' || l_fcast_id);
         update_fcast_header (p_file_id, l_fcast_id, l_rev);
         log_message ('C> completed update fcast_header:' || l_fcast_id);
         pkg_file_loader.create_loader_param (p_file_id,
                                              gc_NEW_REVISION,
                                              L_rev,
                                              gc_VARCHAR2);
      ELSE
         l_fcast_id :=
            PKG_FILE_LOADER.GET_NUMBER_PARAM (P_FILE_ID, gc_fcastid);
         l_rev := create_fcast_override (l_fcast_id);
         log_message (l_category || ' :get new revision' || l_rev);
         PREPARE_LOAD (p_file_id, l_fcast_id, l_rev);
         IMPORT_DATA (p_file_id, l_fcast_id, l_rev);
         update_fcast_header (p_file_id, l_fcast_id, l_rev);
         pkg_file_loader.create_loader_param (p_file_id,
                                              gc_NEW_REVISION,
                                              L_rev,
                                              gc_VARCHAR2);
      END IF;

      pkg_file_loader.process_complete (p_file_id);
      audit_log (p_file_id);
      log_message (
         l_category || ':end data load file_id[' || p_file_id || ']');
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message ('file_id[' || p_file_id || '] error' || err_stack);
         pkg_file_loader.process_error (p_file_id, err_stack);
         ROLLBACK;
         RAISE;
   END;


   PROCEDURE MM_UPLOAD (p_file_id IN NUMBER)
   IS
      l_fcast_id   fcast_header.fcast_id%TYPE;
      l_rev        fcast_header.revision%TYPE;
      l_category   gl_file_type.category%TYPE;
      l_name       TRADINGANALYSIS.gl_FILE_PARAMS.val%TYPE;
   BEGIN
      log_message ('start data load file_id[' || p_file_id || ']');
      l_category :=
         PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, gc_category);
      l_fcast_id := PKG_FILE_LOADER.GET_VARCHAR2_PARAM (P_FILE_ID, gc_fcastid);
      L_REV := create_fcast_revision (l_fcast_id);
      log_message (
            'import data to fcast_id['
         || l_fcast_id
         || '],revision['
         || l_rev
         || ']');
      PREPARE_LOAD (p_file_id, l_fcast_id, l_rev);
      IMPORT_DATA (p_file_id, l_fcast_id, l_rev);
      update_fcast_header (p_file_id, l_fcast_id, l_rev);
      pkg_file_loader.process_complete (p_file_id);
      pkg_file_loader.create_loader_param (p_file_id,
                                           gc_NEW_REVISION,
                                           L_rev,
                                           gc_VARCHAR2);
      log_message ('end data load file_id[' || p_file_id || ']');
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message ('file_id[' || p_file_id || '] error' || err_stack);
         pkg_file_loader.process_error (p_file_id, err_stack);
         ROLLBACK;
         RAISE;
   END;

   FUNCTION create_fcast_override (p_fcast_id IN NUMBER)
      RETURN NUMBER
   IS
      l_revision                 NUMBER;
      l_invalid_fcast_id_range   EXCEPTION;
      PRAGMA EXCEPTION_INIT (l_invalid_fcast_id_range, -20001);
   BEGIN
      --      IF (p_fcast_id < 3000 OR p_fcast_id > 3999)
      --      THEN
      --         RAISE l_invalid_fcast_id_range;
      --      END IF;

      l_revision := create_fcast_revision (p_fcast_id);
      RETURN l_revision;
   EXCEPTION
      WHEN l_invalid_fcast_id_range
      THEN
         raise_application_error (
            -20003,
            'invalid fcast_id range [' || p_fcast_id || ']',
            TRUE);
      WHEN OTHERS
      THEN
         log_message (err_stack);
         RAISE;
   END;


   FUNCTION create_fcast_revision (p_fcast_id IN NUMBER)
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

      log_message (
            'create new revision for (fcast_id,rev)['
         || P_FCAST_ID
         || ','
         || l_REVISION
         || ']');

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
                comments,
                SYSDATE
           FROM fcast_header fh
          WHERE fh.fcast_id = p_fcast_id AND revision = l_revision;

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
         ROLLBACK;
         log_message (err_stack);
         RAISE;
   END;


   /***   PPA section here **/

   PROCEDURE create_ppa_revision (p_name       IN     VARCHAR2,
                                  p_fcast_id      OUT NUMBER,
                                  p_revision      OUT NUMBER)
   IS
   -- This procedure create new revision of a fcast_header record
   --      fcast_not_exist   EXCEPTION;
   --      PRAGMA EXCEPTIONIT (fcast_not_exist, -20001);
   BEGIN
      SELECT fcast_id
        INTO p_fcast_id
        FROM TRADINGANALYSIS.FCAST_HEADER
       WHERE     name = p_name
             AND duration = 'LONG_TERM'
             AND fcast_category = 'PPA'
             AND fcast_type = 'AVERAGE'
             AND ROWNUM <= 1;

      p_revision := create_fcast_revision (p_fcast_id);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error (-20003,
                                  'fcast does not exist [' || p_name || ']',
                                  TRUE);
      WHEN OTHERS
      THEN
         log_message (err_stack);
         RAISE;
   END;


   PROCEDURE audit_log (p_file_id IN NUMBER)
   IS
      l_userid               VARCHAR2 (30);
      l_fcastid              VARCHAR2 (30);
      l_rev                  VARCHAR2 (30);
      l_new_rev              VARCHAR2 (30);
      l_comments             VARCHAR2 (200);
      l_category             TRADINGANALYSIS.GL_FILE_TYPE.CATEGORY%TYPE;
      l_state_load_fcastid   VARCHAR2 (30);
      l_state_load_rev       VARCHAR2 (30);
      l_state_load_new_rev   VARCHAR2 (30);
   BEGIN
      l_userid := PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, GC_USERID);
      l_fcastid := PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, GC_FCASTID);
      l_rev := PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, GC_REVISION);
      l_new_rev :=
         PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, GC_NEW_REVISION);
      l_comments :=
         PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID, GC_COMMENTS);

      INSERT INTO TRADINGANALYSIS.GL_FCAST_OVERRIDE_AUDIT (FCAST_ID,
                                                           OLD_REVISION,
                                                           NEW_REVISION,
                                                           OVERRIDDEN_BY,
                                                           COMMENTS,
                                                           FILE_ID)
           VALUES (l_fcastid,
                   l_rev,
                   l_new_rev,
                   l_userid,
                   l_comments,
                   p_file_id);

      BEGIN
         SELECT category
           INTO l_category
           FROM TRADINGANALYSIS.GL_FILE_LOADER f
                JOIN TRADINGANALYSIS.GL_FILE_TYPE t
                   ON f.file_type_id = t.file_type_id
          WHERE f.file_id = p_file_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      IF l_category = GC_STATE_LOAD
      THEN
         l_state_load_fcastid :=
            PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID,
                                                GC_STATE_LOAD_FCASTID);
         l_state_load_rev :=
            PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID,
                                                GC_STATE_LOAD_REVISION);
         l_state_load_new_rev :=
            PKG_FILE_LOADER.GET_VARCHAR2_param (P_FILE_ID,
                                                GC_STATE_LOAD_NEW_REVISION);

         INSERT INTO TRADINGANALYSIS.GL_FCAST_OVERRIDE_AUDIT (FCAST_ID,
                                                              OLD_REVISION,
                                                              NEW_REVISION,
                                                              OVERRIDDEN_BY,
                                                              COMMENTS,
                                                              FILE_ID)
              VALUES (l_state_load_fcastid,
                      l_state_load_rev,
                      l_state_load_new_rev,
                      l_userid,
                      l_comments,
                      p_file_id);
      END IF;
   END;


   FUNCTION create_fcast_override_request (p_filename    IN VARCHAR2,
                                           p_location    IN VARCHAR2,
                                           p_category    IN VARCHAR2,
                                           p_fcast_id    IN NUMBER,
                                           p_upload_by   IN VARCHAR2,
                                           p_comments    IN VARCHAR2)
      RETURN NUMBER
   IS
      /* this procedure is to override fcast using mm load or ci load or state load */
      l_invalid_fcast_id_range   EXCEPTION;
      l_seq_id                   NUMBER (6);
      l_rev                      fcast_header.revision%TYPE;
      l_os_user                  VARCHAR2 (32);
      l_upload_by                VARCHAR2 (32);
      l_fcast_id                 NUMBER (6);
      PRAGMA EXCEPTION_INIT (l_invalid_fcast_id_range, -20001);
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'OS_USER') INTO l_os_user FROM DUAL;

      l_upload_by := NVL (p_upload_by, l_os_user);
      l_rev := get_revision (P_FCAST_ID);
      l_seq_id :=
         pkg_file_loader.create_file_load_request (p_filename,
                                                   p_location,
                                                   p_category,
                                                   l_upload_by,
                                                   p_comments);


      pkg_file_loader.create_loader_param (l_seq_id,
                                           GC_USERID,
                                           l_upload_by,
                                           GC_VARCHAR2);


      pkg_file_loader.create_loader_param (l_seq_id,
                                           GC_CATEGORY,
                                           p_category,
                                           GC_VARCHAR2);

      pkg_file_loader.create_loader_param (l_seq_id,
                                           GC_FILENAME,
                                           p_filename,
                                           GC_VARCHAR2);

      pkg_file_loader.create_loader_param (l_seq_id,
                                           GC_LOCATION,
                                           p_location,
                                           GC_VARCHAR2);

      pkg_file_loader.create_loader_param (l_seq_id,
                                           GC_COMMENTS,
                                           p_comments,
                                           GC_VARCHAR2);

      IF p_category = GC_STATE_LOAD
      THEN
         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_STATE_LOAD_FCASTID,
                                              p_fcast_id,
                                              GC_VARCHAR2);
         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_STATE_LOAD_REVISION,
                                              l_rev,
                                              GC_VARCHAR2);

         IF p_fcast_id > 30000
         THEN
            l_fcast_id := p_fcast_id / 10;
         END IF;

         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_FCASTID,
                                              l_fcast_id,
                                              GC_VARCHAR2);
         l_rev := get_revision (l_fcast_id);
         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_REVISION,
                                              l_rev,
                                              GC_VARCHAR2);
      ELSE
         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_FCASTID,
                                              p_fcast_id,
                                              GC_VARCHAR2);
         pkg_file_loader.create_loader_param (l_seq_id,
                                              GC_REVISION,
                                              l_rev,
                                              GC_VARCHAR2);
      END IF;


      RETURN l_seq_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack);
         RAISE;
   END;
   
   PROCEDURE c_fcast_override_req (p_filename    IN     VARCHAR2,
                                            p_location    IN     VARCHAR2,
                                            p_category    IN     VARCHAR2,
                                            p_fcast_id    IN     NUMBER,
                                            p_upload_by   IN     VARCHAR2,
                                            p_comments    IN     VARCHAR2,
                                            p_seq_id         OUT NUMBER)
   IS
   BEGIN
      p_seq_id :=
         create_fcast_override_request (p_filename,
                                        p_location,
                                        p_category,
                                        p_fcast_id,
                                        p_upload_by,
                                        p_comments);
   END;

   PROCEDURE create_fcast_override_request (p_filename    IN     VARCHAR2,
                                            p_location    IN     VARCHAR2,
                                            p_category    IN     VARCHAR2,
                                            p_fcast_id    IN     NUMBER,
                                            p_upload_by   IN     VARCHAR2,
                                            p_comments    IN     VARCHAR2,
                                            p_seq_id         OUT NUMBER)
   IS
   BEGIN
      p_seq_id :=
         create_fcast_override_request (p_filename,
                                        p_location,
                                        p_category,
                                        p_fcast_id,
                                        p_upload_by,
                                        p_comments);
   END;


   PROCEDURE create_ppa_fcast_request2 (p_filename    IN     VARCHAR2,
                                        p_location    IN     VARCHAR2,
                                        p_category    IN     VARCHAR2,
                                        p_fcast_id    IN     NUMBER,
                                        p_upload_by   IN     VARCHAR2,
                                        p_comments    IN     VARCHAR2,
                                        p_seq_id         OUT NUMBER)
   IS
   BEGIN
      p_seq_id :=
         create_fcast_override_request (p_filename,
                                        p_location,
                                        p_category,
                                        p_fcast_id,
                                        p_upload_by,
                                        p_comments);
   END;

   PROCEDURE delete_forecast (p_fcast_id IN NUMBER, p_revision IN NUMBER, p_changedby in varchar2)
   IS
   BEGIN
   log_message(p_fcast_id||'.'||p_revision);
      IF p_fcast_id < 3000 OR p_fcast_id > 3999
      THEN
         raise_application_error (
            -20020,
            'cannot delete forecast outside of the 3xxx series',
            TRUE);
      END IF;

      DELETE fcast_detail_hh dd
       WHERE dd.fcast_id = p_fcast_id AND dd.revision = p_revision;

      DELETE fcast_header hh
       WHERE hh.fcast_id = p_fcast_id AND hh.revision = p_revision;
       
      INSERT INTO TRADINGANALYSIS.GL_FCAST_OVERRIDE_AUDIT (FCAST_ID,
                                                           OLD_REVISION,
                                                           NEW_REVISION,
                                                           OVERRIDDEN_BY,
                                                           COMMENTS,
                                                           FILE_ID)
      values(p_fcast_id,p_revision,null,p_changedby,'DELETED OVERRIDE',null);
      COMMIT;
      exception 
      when others then
         rollback;
         raise; 
         
   END;
   
   
   PROCEDURE Toggle_forecast (p_fcast_id IN NUMBER, p_changedby in varchar2)
   IS
      l_status TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING.ACTIVE%type;
   BEGIN
      IF p_fcast_id < 3000 OR p_fcast_id > 3999
      THEN
         raise_application_error (
            -20020,
            'fcast_id outside of the 3xxx series',
            TRUE);
      END IF;
      
      begin
         select active into l_status from TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING m
         where m.fcast_id = p_fcast_id and m.data_source_id = 3;
         exception 
         when no_data_found then
            null;
         when too_many_rows then
             raise_application_error (
            -20023,
            'too many rows returned by fcast_id:'||p_fcast_id,
            TRUE);
         
      end;

      if l_status = 'Y' then
         update TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING 
         set active='N' where fcast_id = p_fcast_id;
      else
         update TRADINGANALYSIS.GL_FCAST_OVERRIDE_MAPPING 
         set active='Y' where fcast_id = p_fcast_id;
      end if;
       
      INSERT INTO TRADINGANALYSIS.GL_FCAST_OVERRIDE_AUDIT (FCAST_ID,
                                                           OLD_REVISION,
                                                           NEW_REVISION,
                                                           OVERRIDDEN_BY,
                                                           COMMENTS,
                                                           FILE_ID)
      values(p_fcast_id,null,null,p_changedby,'UPDATE OVERRIDE ACTIVE FLAG',null);
      COMMIT;
      exception 
      when others then
         rollback;
         raise; 
         
   END;
   
   PROCEDURE get_fcastoverride_vol (p_fcast_id IN NUMBER, out_load_request      OUT SYS_REFCURSOR)
   is
   begin
      open out_load_request for
     select fcast_id,revision
      ,datetime
      ,round(quantity_mw,2) quantity_mw,
      (SELECT  m.name
     FROM TRADINGANALYSIS.gl_FCAST_OVERRIDE_MAPPING m
    WHERE m.fcast_id = p_fcast_id and rownum<=1) name
      from fcast_detail_hh where fcast_id = p_fcast_id;

      
   end;
   
   
   PROCEDURE get_fcast_volume (p_fcast_id IN NUMBER,p_revision IN NUMBER,  out_load_request      OUT SYS_REFCURSOR)
   is
   begin
      open out_load_request for
     select fcast_id,revision
      ,trunc(datetime-1/48,'MON') datetime
      ,round(sum(quantity_mw),2)/1000 quantity_gw,
      (SELECT  m.name
     FROM TRADINGANALYSIS.fcast_header m
    WHERE m.fcast_id = p_fcast_id and m.revision=p_revision) name
      from fcast_detail_hh where fcast_id = p_fcast_id AND revision=p_revision
      group by fcast_id,revision
      ,trunc(datetime-1/48,'MON')
      order by 3;

      
   end;
   
   
   PROCEDURE get_fcast_volume_avg (p_fcast_id IN NUMBER,p_revision IN NUMBER,  out_load_request      OUT SYS_REFCURSOR)
   is
   begin
      open out_load_request for
    select fcast_id,revision,
           avg(quantity_mw) avg_mw,count(datetime) record_count,min(quantity_mw) min_mw,max(quantity_mw) max_mw
      from fcast_detail_hh where fcast_id = p_fcast_id AND revision=p_revision
      group by fcast_id,revision;

      
   end;
   
  
   
    PROCEDURE get_ppa_header (p_state in varchar2,  out_load_request      OUT SYS_REFCURSOR)
   is
   begin
      open out_load_request for
   
   /* Formatted on 22/05/2015 16:57:52 (QP5 v5.269.14213.34769) */
SELECT PPA_ID,
       NAME,
       FUEL_TYPE,
       PORTFOLIO,
       COMMODITY,
       SUPPLY_CATEGORY,
       COUNTERPARTY,
       DUID,
       STATE,
       OE_ENTITY,
       IM_FLAG,
       CAPACITY_MW,
       CAPACITY_PERC,
       MARKET_REGD_GEN_FLAG,
       MARKET_SETTLED_FLAG,
       IND_RATE_PEAK,
       IND_RATE_OFFPEAK,
       CONTRACT_ANNUAL_MWH,
       FORECAST_ANNUAL_MWH,
       CARBON_INTENSITY,
       GPR_DATE_FROM,
       GPR_DATE_TO,
       CONTRACT_DESC
  FROM ppa_master mm
  where (mm.state = p_state or p_state is null)
  order by name;

      
   end;
   
   PROCEDURE update_fcast_header (p_fcast_id IN NUMBER, p_rev in number, p_exposure_upload in varchar2, p_comments in varchar2, p_changedby in varchar2)
   is
   v_prev_rev fcast_header.revision%type;
   begin
   
      if p_exposure_upload = 'Y' then 
   
      begin
      select revision into v_prev_rev from fcast_header where fcast_id = p_fcast_id and exposure_upload='Y';
      exception 
      when too_many_rows then
         LOG_MESSAGE('Error: more than one fcast_header with exposure_upload=Y:'||p_fcast_id);
         raise;
      when no_data_found then
         LOG_MESSAGE('Error: cannotmore than one fcast_header with exposure_upload=Y:'||p_fcast_id); 
      end;
      
      update fcast_header set comments = nvl(p_comments,comments),exposure_upload='Y' WHERE
      FCAST_ID = P_FCAST_ID AND REVISION=P_REV;
      
      update fcast_header set exposure_upload='N' WHERE
      FCAST_ID = P_FCAST_ID AND REVISION=v_prev_rev;
      
      if sql%rowcount <> 1 then
      
       LOG_MESSAGE('Error: unable to set exposure_upload=N for fcast_header :'||p_fcast_id || ' rev:'||v_prev_rev);
      
      end if;
      
      else
      
        update fcast_header set exposure_upload='N' WHERE
      FCAST_ID = P_FCAST_ID AND REVISION=P_REV;
      
      end if;
      
      
       
   
   end;
   
END pkg_fcast_maint;
/