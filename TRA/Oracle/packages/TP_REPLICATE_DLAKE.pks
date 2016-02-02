CREATE OR REPLACE PACKAGE TRADINGANALYSIS.TP_REPLICATE_DLAKE
IS
   g_c_caller   VARCHAR2 (100) := 'TP_REPLICATE_DLAKE';
   g_program   VARCHAR2 (100) := 'TP_REPLICATE_DLAKE';
   g_source varchar2(10):='SANDPIT2';
   g_schema varchar2(30):='TRADINGANALYSIS';
   q varchar2(10):=q'[']';
   
   

   
FUNCTION err_stack      RETURN VARCHAR2;


procedure exec_sql(v_text in varchar2);
                              
PROCEDURE gather_stats(in_table_name in varchar2); 

procedure load_table(p_owner in varchar2, p_src_table_name in varchar2, p_dest_table_name in varchar2,p_db_link  in varchar2, p_mode in varchar2 default 'append', p_data_source in varchar2);
procedure create_tables(p_data_source in varchar2, p_db_link in varchar2);   
procedure load_tables(p_data_source in varchar2, p_db_link in varchar2, p_fast in boolean default false);
--procedure create_view(p_table_name in varchar2);
   procedure validate_data(p_table_name in varchar2,p_check_rows in number default 100);
   PROCEDURE transform_table (p_table_name   IN VARCHAR2);
                            

end;
/