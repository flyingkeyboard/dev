CREATE OR REPLACE PACKAGE TEMP_DATA.PKG_TP_snapshot_extra
IS
   g_c_caller   VARCHAR2 (100) := 'PKG_TP_snapshot_extra';
   g_ppa_category number(2):=3;
   g_ci_category number(2):=2;
   g_setcp_category number(2):=1;
   g_mm_category number(2):=4;
   
   

 
FUNCTION err_stack      RETURN VARCHAR2;

PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2, p_type IN VARCHAR2);

procedure calc_ci_tni(in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE);
                                   
procedure calc_ppa_tni(in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE);
                                   

procedure run(in_snapshot_id   IN NUMBER,
                                   in_start_date    IN DATE,
                                   in_finish_date   IN DATE); 
                                   
PROCEDURE calc_mm_tni (in_snapshot_id   IN NUMBER,
                          in_start_date    IN DATE,
                          in_finish_date   IN DATE);                                    
                                   
FUNCTION get_tni(p_nmi varchar2)
      RETURN VARCHAR2; 
      
      
                                      
   

end;
/