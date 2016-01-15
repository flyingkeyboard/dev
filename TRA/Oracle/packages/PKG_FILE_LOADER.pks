CREATE OR REPLACE PACKAGE TRADINGANALYSIS.PKG_FILE_LOADER
AS
   /******************************************************************************
      NAME:       pkg_fcast_override
      PURPOSE:

      REVISIONS:
      Ver        Date        Author           Description
      ---------  ----------  ---------------  ------------------------------------
      1.0        02/02/2015      taop       1. Created this package.
   ******************************************************************************/

   GC_PROCESS_PENDING           CONSTANT VARCHAR2 (30) := 'PENDING';
   GC_PROCESS_SUCCESS           CONSTANT VARCHAR2 (30) := 'SUCCESS';
   GC_PROCESS_FAILED            CONSTANT VARCHAR2 (30) := 'FAILED';
   GC_PARAM_DATE_FORMAT         CONSTANT VARCHAR2 (50) := 'dd-Mon-yyyy hh24:mi:ss';

   GC_PARAM_SHORT_DATE_FORMAT   CONSTANT VARCHAR2 (50) := 'dd-Mon-yyyy';
   GC_INVALID_FCASTID_EXC       CONSTANT NUMBER (6) := -20010;
   GC_NO_ROW_LOADED_EXC         CONSTANT NUMBER (6) := -200020;
   GC_NO_ROW_LOADED_EXC         CONSTANT NUMBER (6) := -200020;

   TYPE AssocArrayVarchar2_t IS TABLE OF VARCHAR (200)
      INDEX BY BINARY_INTEGER;
      
  FUNCTION create_file_load_request (p_filename    IN VARCHAR2,
                                      p_location    IN VARCHAR2,
                                      p_category    IN VARCHAR2,
                                      p_upload_by   IN VARCHAR2,
                                      p_upload_comment   IN VARCHAR2
                                      )
      RETURN NUMBER;

   PROCEDURE log_error_message (p_file_id   IN NUMBER,
                                p_message   IN AssocArrayVarchar2_t);

   FUNCTION err_stack
      RETURN VARCHAR2;

   PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2);
  

   FUNCTION get_param (p_file_id   IN GL_file_params.file_id%TYPE,
                       p_name      IN GL_file_params.name%TYPE)
      RETURN gl_file_params%ROWTYPE;

   FUNCTION get_date_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN DATE;

   FUNCTION get_number_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN NUMBER;

   -- Returns a varchar2 value from the parameter table
   FUNCTION get_varchar2_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE)
      RETURN VARCHAR2;

   PROCEDURE process_complete (p_file_id IN NUMBER);

   PROCEDURE process_error (p_file_id IN NUMBER, p_errmsg IN VARCHAR2);

   PROCEDURE get_file_load_Request (out_load_request OUT SYS_REFCURSOR);
   
   PROCEDURE get_file_load_errors (p_file_id in number, out_load_request OUT SYS_REFCURSOR);

 

   PROCEDURE create_loader_param (
      p_file_id   IN gl_file_params.file_id%TYPE,
      p_name      IN gl_file_params.name%TYPE,
      p_val       IN gl_FILE_PARAMS.VAL%TYPE,
      p_type      IN VARCHAR2);
      
      PROCEDURE check_file_load_request (
      p_userid        IN     VARCHAR2,
      out_load_request      OUT SYS_REFCURSOR);
END PKG_FILE_LOADER;
/