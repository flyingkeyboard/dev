CREATE OR REPLACE PACKAGE BODY TRADINGANALYSIS.PKG_FILE_LOADER
AS
   /******************************************************************************
      NAME:       pkg_fcast_override
      PURPOSE:

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        02/02/2015      taop       1. Created this package.
   ******************************************************************************/
     PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2, p_type in varchar2)
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      c_s_err varchar2(10):='ERROR:';
      c_s_info varchar2(10):='';
      v_message tradinganalysis.message_log.message%type;
      user_name   VARCHAR2 (60);
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'OS_USER') INTO user_name FROM DUAL;
      
      if p_type = 'ERROR' THEN
         v_message:= c_s_err||p_message;
      else
         v_message:= c_s_info||p_message;
      end if;      

      INSERT INTO tradinganalysis.message_log (TIME, MESSAGE, USERID)
           VALUES (SYSDATE, v_message, user_name);

      COMMIT;
   END;
   
   PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2)
   AS
   begin
      LOG_MESSAGE (p_message,'INFO');
   end;


  

   
   
--   PROCEDURE raise_exception_error (
--      p_errcode in number,
--      p_message in varchar2,
--      p_reraise_error in boolean default null)
--   IS
--   begin
--      log_message(err_stack,'ERROR');
--      log_message(P_MESSAGE,'ERROR');
--      if p_reraise_error = true then
--           raise_application_error (
--               p_errcode,
--               p_message,
--               TRUE);
--      end if;
--         
--      
--   end;

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



   -- Returns a row from the parameter table
   FUNCTION get_param (p_file_id   IN GL_file_params.file_id%TYPE,
                       p_name      IN gl_file_params.name%TYPE)
      RETURN gl_file_params%ROWTYPE
   IS
      l_param_row   gl_file_params%ROWTYPE;
   BEGIN
      SELECT *
        INTO l_param_row
        FROM gl_file_params
       WHERE file_id = p_file_id AND name = p_name;

      RETURN l_param_row;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
      raise_application_error(
            -20001,
               'No parameters in gl_file_params have file_id equal to ['
            || p_file_id
            || '] and name ['
            || p_name
            || ']',true);
   END;

   -- Converts and returns a date value from the parameter table
   FUNCTION get_date_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN DATE
   IS
      l_param   gl_file_params.val%TYPE;
   BEGIN
      l_param := get_param (p_file_id, p_name).val;

      BEGIN
         log_message (
            'return param: ' || TO_DATE (l_param, GC_PARAM_DATE_FORMAT));
         RETURN TO_DATE (l_param, GC_PARAM_DATE_FORMAT);
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (
               -20002,
               'Could not convert parameter [' || l_param || '] to date',
               TRUE);
      END;
   END;

   -- Converts and returns a number value from the parameter table
   FUNCTION get_number_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN NUMBER
   IS
      l_param   gl_file_params.val%TYPE;
   BEGIN
      l_param := get_param (p_file_id, p_name).val;

      BEGIN
         log_message ('return param: ' || TO_NUMBER (l_param));
         RETURN TO_NUMBER (l_param);
      EXCEPTION
         WHEN OTHERS
         THEN
            raise_application_error (
               -20002,
               'Could not convert parameter [' || l_param || '] to number',
               TRUE);
      END;
   END;

   -- Returns a varchar2 value from the parameter table
   FUNCTION get_varchar2_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN get_param (p_file_id, p_name).val;
   END;

 


   FUNCTION create_file_load_request (p_filename    IN VARCHAR2,
                                      p_location    IN VARCHAR2,
                                      p_category    IN VARCHAR2,
                                      p_upload_by   IN VARCHAR2,
                                      p_upload_comment   IN VARCHAR2
                                      )
      RETURN NUMBER
   IS
      invalid_fcast_id_range   EXCEPTION;
      l_seq_id                 NUMBER (6);
      l_file_type_id           gl_FILE_TYPE.FILE_TYPE_ID%TYPE;
      l_data_dir gl_file_type.data_file_path%type;
      l_os_user varchar2(32);
      PRAGMA EXCEPTION_INIT (invalid_fcast_id_range, -20001);
   BEGIN
   
      SELECT sys_context('USERENV', 'OS_USER') into l_os_user FROM DUAL;
      SELECT seq_file_id.NEXTVAL INTO l_seq_id FROM DUAL;


      BEGIN
         SELECT file_type_id,data_file_path
           INTO l_file_type_id, l_data_dir
           FROM TRADINGANALYSIS.gl_FILE_TYPE
          WHERE (category = p_category);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
--            log_message (
--                  'Error: cannot find the correct data category type:'
--               || p_category
--               || ' does not exist. expecting MASS_MARKET,CI');
             raise_application_error (
               -20012,
               'Error: cannot find the correct data category type ['  || p_category || ']'
               || ' does not exist. expecting MASS_MARKET,CI',
               true);
          
      END;
      
      

      INSERT INTO gl_FILE_LOADER (FILE_ID,
                                       FILENAME,
                                       FILE_DIR,
                                       UPLOADED_BY,
                                       UPLOAD_DATE,
                                       STATUS,
                                       FILE_TYPE_ID,
                                       UPLOAD_COMMENTS)
           VALUES (l_seq_id,
                   p_filename,
                   nvl(p_location,l_data_dir),
                   nvl(p_upload_by,l_os_user),
                   SYSDATE,
                   GC_PROCESS_PENDING,
                   l_file_type_id,
                   p_upload_comment
                   );
      RETURN l_seq_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         log_message (err_stack,'ERROR');
         rollback;
         RAISE;
   END;




   PROCEDURE process_complete (p_file_id IN NUMBER)
   IS
   BEGIN
      UPDATE TRADINGANALYSIS.gl_FILE_LOADER
         SET processing_date = SYSDATE,
             status = GC_PROCESS_SUCCESS,
             upload_errmsg = NULL
       WHERE file_id = p_file_id;

      --audit_upload;
      log_message('completed loading file_id['||p_file_id||']');
      
      delete from TRADINGANALYSIS.GL_STAGING_VOLUME;
      delete from TRADINGANALYSIS.GL_LOAD_VOLUME;
      COMMIT;
   END;

   PROCEDURE process_error (p_file_id IN NUMBER, p_errmsg IN VARCHAR2)
   IS
   PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      UPDATE TRADINGANALYSIS.gl_FILE_LOADER
         SET processing_date = SYSDATE,
             status = GC_PROCESS_FAILED,
             UPLOAD_ERRMSG = SUBSTR (P_ERRMSG, 1, 200)
       WHERE file_id = p_file_id;
       
      insert into  TRADINGANALYSIS.GL_FILE_LOADER_ERROR(FILE_ID, ERROR_MESSAGE, MESSAGE_ID, DATETIME)
      values(p_file_id,substr(p_errmsg,1,500),99,sysdate);
      
      log_message('failed loading file_id['||p_file_id||'] see gl_file_loader_error for more details');

      COMMIT;
   END;


   PROCEDURE get_file_load_Request (out_load_request OUT SYS_REFCURSOR)
   IS
   BEGIN
   
      OPEN out_load_request FOR
         SELECT fl.file_id,
                fl.filename,
                file_dir,
                method_type,
                executable_path,
                control_file,
                post_process_proc
           FROM TRADINGANALYSIS.gl_FILE_LOADER fl
                JOIN TRADINGANALYSIS.gl_FILE_TYPE ty
                   ON fl.file_type_id = ty.file_type_id
          WHERE status = 'PENDING';
          
          log_message('checking file_id with pending status');
   END;
   
   PROCEDURE get_file_load_errors (p_file_id in number, out_load_request OUT SYS_REFCURSOR)
   IS
   BEGIN
      OPEN out_load_request FOR
       SELECT FILE_ID, DATETIME, MESSAGE_ID,ERROR_MESSAGE 
           FROM TRADINGANALYSIS.GL_FILE_LOADER_ERROR fl
           where fl.file_id = p_file_id
           order by file_id desc, message_id desc;
  
   END;




   PROCEDURE check_file_load_request (
      p_userid        IN     VARCHAR2,
      out_load_request      OUT SYS_REFCURSOR)
   IS
   l_error_count number(3);
   BEGIN
      OPEN out_load_request FOR
           SELECT fl.file_id,
                  file_dir || fl.filename filename,
                  status,
                  uploaded_by,
                  upload_date,
                  processing_date,
                  upload_comments,
                  (select error_message from TRADINGANALYSIS.GL_FILE_LOADER_ERROR fe 
                  where fe.file_id = fl.file_id and rownum <=1) upload_errmsg
             FROM TRADINGANALYSIS.gl_FILE_LOADER fl
                  JOIN TRADINGANALYSIS.gl_FILE_TYPE ty
                     ON fl.file_type_id = ty.file_type_id
                     and fl.upload_date > trunc(sysdate-5)
                   --  and uploaded_by = p_userid
         ORDER BY 1 DESC;
   END;


   PROCEDURE log_error_message (p_file_id   IN NUMBER,
                                p_message   IN AssocArrayVarchar2_t)
   IS
   BEGIN
      FOR cnt IN 1 .. p_message.COUNT
      LOOP
         IF LENGTH (p_message (cnt)) > 1
         THEN
            INSERT
              INTO TRADINGANALYSIS.gl_FILE_LOADER_ERROR (FILE_ID,
                                                              ERROR_MESSAGE,
                                                              message_id)
            VALUES (p_file_id, p_message (cnt), cnt);
         END IF;
      END LOOP;
      
      process_error (
         p_file_id,
         p_message(1));
   END;

   PROCEDURE create_loader_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE,
      p_val       IN gl_FILE_PARAMS.VAL%TYPE,
      p_type      IN VARCHAR2)
   IS
   BEGIN
      INSERT INTO TRADINGANALYSIS.gl_FILE_PARAMS (FILE_ID,
                                                       NAME,
                                                       VAL,
                                                       DATA_TYPE)
           VALUES (p_file_id,
                   p_name,
                   p_val,
                   p_type);
   END;
   
 
   
END PKG_FILE_LOADER;
/