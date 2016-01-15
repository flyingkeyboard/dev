CREATE OR REPLACE PACKAGE TEMP_DATA.PKG_TP_Snapshot
IS
   g_c_caller   VARCHAR2 (100) := 'PKG_TP_SNAPSHOT';
   g_source varchar2(10):='NETSREPP';

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
                          in_cleanup_act        IN VARCHAR2 DEFAULT 'Y');
                          
                          PROCEDURE LOG_MESSAGE (p_message IN VARCHAR2, p_type in varchar2);
FUNCTION err_stack      RETURN VARCHAR2;

procedure snapshot_contracts(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure snapshot_hh_retail(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure snapshot_retail_site(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure snapshot_ref_tables(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure snapshot_sitemeterdata(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure backfill_agg_site_hh(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure load_agg_site_hh(in_snapshot_id in number,in_start_date in date,in_finish_date in date, in_siteid in number);
procedure apply_load_profile(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure snapshot_sitemeterdata(in_snapshot_id in number,in_start_date in date,in_finish_date in date, in_siteid in number);
procedure apply_meterdata(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
procedure gather_stats;

 FUNCTION Get_Default_Tariff (in_site_id        NUMBER,
                                in_start_date           DATE,
                                in_finish_date           DATE,
                                in_snapshot_id    NUMBER)
      RETURN VARCHAR2;
      
      FUNCTION Get_Default_Tariff_O (in_site_id        NUMBER,
                                in_date           DATE,
                                in_snapshot_id    NUMBER)
      RETURN VARCHAR2;

 PROCEDURE Create_CID_Records (in_snapshot_id   IN NUMBER,
                                 in_start_date    IN DATE,
                                 in_finish_date   IN DATE);
                                 
procedure snapshot_load_profile(in_snapshot_id in number,in_start_date in date,in_finish_date in date);
                             procedure cleanup_act;


                          

PROCEDURE snapshot_setcpdata (in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE);



PROCEDURE agg_mass_market (in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE);
                                     
procedure additional_adjustment(in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE);
                                     
PROCEDURE get_mm_volume_hh (in_snapshot_id in number, out_load_request OUT SYS_REFCURSOR);

PROCEDURE get_mm_volume_month (in_snapshot_id     IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR);

PROCEDURE get_ci_volume (p_snapshot_id      IN     NUMBER,
                            out_load_request      OUT SYS_REFCURSOR);
                            
PROCEDURE run_snapshot ;

PROCEDURE get_ppa_volume (p_snapshot_id      IN     NUMBER,
                                     out_load_request      OUT SYS_REFCURSOR);
                                     


procedure get_netsrepp_data(in_snapshot_id   IN NUMBER,
                                     in_start_date    IN DATE,
                                     in_finish_date   IN DATE);
                                     
procedure schedule_job(p_start_date IN DATE, out_load_request      OUT SYS_REFCURSOR);

PROCEDURE refresh_views(in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE);
                              
PROCEDURE gather_stats(in_table_name in varchar2);

PROCEDURE run_snapshot(in_snapshot_id   IN NUMBER,
                              in_start_date    IN DATE,
                              in_finish_date   IN DATE); 

   

end;
/