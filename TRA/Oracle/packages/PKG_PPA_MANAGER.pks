CREATE OR REPLACE PACKAGE TRADINGANALYSIS.PKG_PPA_MANAGER AS

procedure P_CREATE_PPA_FCAST_HEADER(p_name in varchar2, p_fcast_id out number, p_revision out number ); 

PROCEDURE P_CREATE_PPA_FCAST_HEADER(p_new_name in varchar2,p_state in varchar2,  p_FCAST_CATEGORY in varchar2, p_FCAST_TYPE in varchar2, p_DURATION in varchar2 default 'LONG_TERM', p_fcast_id out number, p_revision out number);



PROCEDURE P_MAIN(p_name in varchar2,p_expect_rec in number);

PROCEDURE P_SAVE(p_fcast_id in number, p_revision in number, p_expect_rec in number);

PROCEDURE P_DELETE(p_fcast_id in number, p_revision in number);

PROCEDURE P_CHECK(p_fcast_id in number, p_revision in number);



PROCEDURE P_CREATE_RATES(p_ppa_id in number, p_start_date in date, p_finish_date in date, p_rate in number, p_rate_type in varchar2);

PROCEDURE P_UPDATE_RATE(p_ppa_name in varchar2, p_start_date in date, p_rate in number, p_rate_type in varchar2);

PROCEDURE P_UPDATE_RATE(p_ppa_id in number, p_start_date in date, p_rate in number, p_rate_type in varchar2);

PROCEDURE P_NEW_PPA(p_name in varchar2,p_state in varchar2, p_expect_rec in number);

FUNCTION create_fcast_revision (p_fcast_id IN NUMBER, p_comment in varchar2) return number;

PROCEDURE P_SAVE_FCAST(p_fcast_id in number, p_revision in number, p_expect_rec in number);

PROCEDURE P_LOAD_FCAST(p_scenario in varchar2, p_comment in varchar2);

/* Formatted on 21/08/2015 15:23:12 (QP5 v5.269.14213.34769) */


PROCEDURE p_create_ppa_master (p_name              IN VARCHAR2,
                               state             IN VARCHAR2 DEFAULT NULL,
                               FUEL_TYPE         IN VARCHAR2 DEFAULT NULL,
                               PORTFOLIO         IN VARCHAR2  DEFAULT NULL,
                               COMMODITY         IN VARCHAR2  DEFAULT NULL,
                               SUPPLY_CATEGORY   IN VARCHAR2 DEFAULT NULL,
                               COUNTERPARTY      IN VARCHAR2 DEFAULT NULL,
                               DUID              IN VARCHAR2 DEFAULT NULL,
                               OE_ENTITY         IN VARCHAR2 DEFAULT NULL,
                               cAPACITY_MW       IN VARCHAR2 DEFAULT NULL,
                               CAPACITY_PERC     IN VARCHAR2  DEFAULT NULL,
                               contract_desc    in varchar2 default null);
                               
procedure create_fcast_data(p_ppa_name IN VARCHAR2, p_start_date in date, p_finish_date in date, p_scaling in number default 1, p_ref_start_date in date);

PROCEDURE refresh_ppa_hh_tni_ACTUAL(p_last_run_date in date default null);

PROCEDURE refresh_ppa_hh_tni_forecast(p_last_run_date in date default null);

END;
/