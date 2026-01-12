FUNCTION     NBKNQ_Ver1(P_BRANCH varchar2,
                 P_REF_NO varchar2,
                 P_CCY    varchar2,
                 P_TUNGAY date,               
                 P_TYPE   varchar2) RETURN SYS_REFCURSOR AS
    resData  SYS_REFCURSOR;
    SoDuDau  number;
    SoDuCuoi number;
    so_du_cuoi_FCC number;
    PREDATE  CHAR(8);
    v_count  int;
  BEGIN
    ---- kiem tra so luong :
    SELECT count(1)
      into v_count
      FROM hoc_tap_fcc_new.SRV_TB_CH_TILL_TOT /* OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
     WHERE to_char(BRANCH_DATE, 'yyyymmdd') <=
           to_char(P_TUNGAY, 'yyyymmdd')
       and TILL_VAULT_INDICATOR = 'V'
       and (instr(it_fn_get_str_br(P_BRANCH), BRANCH_CODE) > 0)
          --AND BRANCH_CODE = P_BRANCH
       and currency_code = P_CCY;

    IF REGEXP_COUNT(P_TYPE, 'INFO') >= 1 THEN
      BEGIN
        ---SoDuDau------------------
        SoDuDau := 0;
        if v_count >= 2 then
          select sum(CURRENT_BALANCE) --add sum vi biet dau n? xuat hien 2 row
            into SoDuDau
            from (SELECT ID,
                         NVL(CURRENT_BALANCE, 0) AS CURRENT_BALANCE,
                         BRANCH_CODE,
                         BRANCH_DATE,
                         ROW_NUMBER() OVER(PARTITION BY BRANCH_CODE ORDER BY BRANCH_DATE desc) row_num
                    FROM hoc_tap_fcc_new.SRV_TB_CH_TILL_TOT /* OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
                   WHERE to_char(BRANCH_DATE, 'yyyymmdd') <
                         to_char(P_TUNGAY, 'yyyymmdd')
                     and TILL_VAULT_INDICATOR = 'V'
                     and (instr(it_fn_get_str_br(P_BRANCH), BRANCH_CODE) > 0)
                        --AND BRANCH_CODE = P_BRANCH
                     and currency_code = P_CCY)
           where row_num = 1; --truonghm edit 0505 sua 2 thanh 1 va dieu kien <= thanh <

        end if;

        /*SELECT nvl(OPENING_BALANCE,0) into SoDuDau
        FROM OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
          WHERE to_char(BRANCH_DATE, 'yyyymmdd') =PREDATE
                and teller_id = 'HAVUTT'
                and CURRENCY_CODE = P_CCY; */
        ---SoDuCuoi------------------
        SoDuCuoi := 0;
        select (SoDuDau + NVL(sum(THU), 0) - NVL(sum(CHI), 0))
          into SoDuCuoi
          FROM (SELECT nvl(tnx.inflow, 0) AS THU, nvl(tnx.outflow, 0) AS CHI
                  FROM hoc_tap_fcc_new.Srv_Tb_Ch_Till_Txn tnx /* Obbr_Remo_Cmn.Srv_Tb_Ch_Till_Txn@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
                  join (select FUNCTION_CODE, TXN_REF_NO, ACCOUNT_NUMBER
                         from (select FUNCTION_CODE,
                                      TXN_REF_NO,
                                      ACCOUNT_NUMBER,
                                      RANK() OVER(PARTITION BY TXN_REF_NO ORDER BY TXN_TIME_RECEIVED DESC) AS RANK_NUM
                                 from hoc_tap_fcc_new.Srv_Tb_Bc_Ej_Log /* Obbr_Remo_Cmn.Srv_Tb_Bc_Ej_Log@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
                                WHERE function_code not in
                                      ('9005', '9006', '9007', '9008'))
                        where RANK_NUM = 1) tn
                    on tnx.TRN_REF_NO = tn.TXN_REF_NO
                  join (SELECT FUNCTION_CODE, FUNCTION_CODE_DESC
                         FROM hoc_tap_fcc_new.SRV_TM_BC_FUNCTION_INDICATOR) tbf /* OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
                    on tbf.function_code = tn.function_code
                 where tn.function_code not in
                       ('9005', '9006', '9007', '9008')
                   and to_char(tnx.branch_date, 'yyyymmdd') =
                       to_char(P_TUNGAY, 'yyyymmdd')
                   and (upper(P_REF_NO) = 'ALL' OR tnx.trn_ref_no = P_REF_NO)
                   and tnx.CURRENCY_CODE = P_CCY
                   and (instr(it_fn_get_str_br(P_BRANCH), tnx.branch_code) > 0));
  --FCC so du
 if P_CCY = 'VND' then

   -- open RESDATA for
  WITH V_SO_DU_DAU AS (
  SELECT nvl((close_bal * -1),0) so_du_dau
              FROM ODS.t1080_tb_gl_bal_eod
              WHERE bkg_date = (select max(bkg_date) from ODS.t1080_tb_gl_bal_eod where bkg_date <= P_TUNGAY - 1)
                 AND ac_no = '101101001'
                 AND branch_code = P_BRANCH
                 AND ac_ccy = P_CCY
                 AND ROWNUM = 1 )
   ,V_TONG_PS AS (
      SELECT SUM(ghi_no) AS tong_ghi_no, SUM(ghi_co) AS tong_ghi_co
      FROM (SELECT external_ref_no AS so_giao_dich,
                             trn_ref_no AS so_but_toan,
               DECODE(drcr_ind,'D',lcy_amount,0) AS ghi_no,
                             DECODE(drcr_ind,'C',lcy_amount,0) AS ghi_co,
                             ac_ccy AS tien_te,
                             SUBSTR(external_ref_no, 1, 3) AS ma_chi_nhanh,
                             user_id
                       FROM hoc_tap_fcc.acvw_all_ac_entries /* vndklb.acvw_all_ac_entries@fcpdb */
                       WHERE ac_no = '101101001'
                         AND ac_ccy = P_CCY
                         AND ac_branch = P_BRANCH
                         AND trn_dt = P_TUNGAY
                         --AND (P_USER is null or user_id = P_USER)
             AND user_id <> 'SYSTEM'
             )
     )

      select distinct so_du_cuoi into so_du_cuoi_FCC
      From(

      SELECT
           DISTINCT NVL((SELECT so_du_dau FROM V_SO_DU_DAU),0) so_du_dau,
       SUBSTR(so_giao_dich,1,16) so_giao_dich,
             so_but_toan,
             ghi_no,
             ghi_co,
             tien_te,
             ma_chi_nhanh,
             user_id,

       NVL((SELECT tong_ghi_no FROM V_TONG_PS),0) tong_ghi_no,
       NVL((SELECT tong_ghi_co FROM V_TONG_PS),0) tong_ghi_co,
       NVL((SELECT so_du_dau FROM V_SO_DU_DAU),0) + NVL((SELECT tong_ghi_no FROM V_TONG_PS),0) - NVL((SELECT tong_ghi_co FROM V_TONG_PS),0) so_du_cuoi,
       FUNCTION_CODE_DESC Description
      FROM (
           SELECT  so_giao_dich,
                     so_but_toan,
                     ghi_no,
                     ghi_co,
                     tien_te,
                     ma_chi_nhanh,
                     A.user_id,
           C.FUNCTION_CODE_DESC
                FROM (
            SELECT so_giao_dich,
                   so_but_toan,
                   SUM(ghi_no) ghi_no,
                 SUM(ghi_co) ghi_co,
                 tien_te,
                 ma_chi_nhanh,
                 user_id
            FROM (
                SELECT   CASE WHEN A.EVENT = 'DEBK' THEN C.TXN_REF_NO
                        WHEN A.EVENT = 'DTOP' THEN D.TXN_REF_NO
                        WHEN A.EVENT = 'INIT' AND A.TRN_CODE = 'CGC' THEN E.TXN_REF_NO
                        ELSE external_ref_no
                     END AS so_giao_dich,
                     trn_ref_no AS so_but_toan,
                     DECODE(drcr_ind,'D',lcy_amount,0) AS ghi_no,
                     DECODE(drcr_ind,'C',lcy_amount,0) AS ghi_co,
                     ac_ccy AS tien_te,
                     SUBSTR(external_ref_no, 1, 3) AS ma_chi_nhanh,
                     A.user_id
                FROM hoc_tap_fcc.acvw_all_ac_entries A /* vndklb.acvw_all_ac_entries@fcpdb */
                LEFT JOIN hoc_tap_fcc.SRV_TB_CH_TD_ACCOUNT C on a.RELATED_ACCOUNT = C.td_ac_no /* OBBR_REMO_TDS.SRV_TB_CH_TD_ACCOUNT@fcpdb */

        LEFT JOIN hoc_tap_fcc.SRV_TB_BC_TXN_LOG D /* OBBR_REMO_CMN.SRV_TB_BC_TXN_LOG@fcpdb */
                      ON  A.EXTERNAL_REF_NO = json_value(D.OUTGOING_RES, '$.data.dataPayload.topUpRefNo' RETURNING VARCHAR2(32))
                        And Trunc(D.req_date) = a.trn_dt
                LEFT JOIN hoc_tap_fcc.SRV_TB_BC_TXN_LOG E /* OBBR_REMO_CMN.SRV_TB_BC_TXN_LOG@fcpdb */
                      ON A.EXTERNAL_REF_NO =  json_value(E.OUTGOING_RES, '$.data.dataPayload.redeemRefNo' RETURNING VARCHAR2(32))
                       AND Trunc(E.req_date) = a.trn_dt

        WHERE ac_no = '101101001'
                    AND ac_ccy = P_CCY
                    AND ac_branch = P_BRANCH
                    AND trn_dt = P_TUNGAY
                    --AND (P_USER is null or A.user_id = P_USER)
                    AND A.user_id <> 'SYSTEM'
               )
               GROUP BY so_giao_dich,
                   so_but_toan,
                   tien_te,
                   ma_chi_nhanh,
                   user_id

            ) A
        --VINHNH20240731 ITS_113767 LEFT JOIN hoc_tap_fcc.SRV_TB_AD_CENTRAL_TXN_LOG B ON SUBSTR(A.so_giao_dich,1,16) = B.TXN_REF_NO /* OBBR_REMO_ADP.SRV_TB_AD_CENTRAL_TXN_LOG@fcpdb */
    LEFT JOIN hoc_tap_fcc.SRV_TB_BC_EJ_LOG B ON SUBSTR(A.so_giao_dich,1,16) = B.TXN_REF_NO --VINHNH20240731 ITS_113767 /* OBBR_REMO_CMN.SRV_TB_BC_EJ_LOG@fcpdb */
        LEFT JOIN hoc_tap_fcc.SRV_TM_BC_FUNCTION_INDICATOR C ON C.FUNCTION_CODE = B.FUNCTION_CODE /* OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB */

      )
      --RIGHT JOIN V_SO_DU_DAU ON 1=1
      RIGHT JOIN V_TONG_PS ON 1=1
       ORDER BY so_giao_dich
     ) ;

  else
    --ngoai te

    --open RESDATA for

   WITH V_SO_DU_DAU AS (
        SELECT nvl((close_bal * -1),0) so_du_dau
              FROM ODS.t1080_tb_gl_bal_eod
              WHERE bkg_date = (select max(bkg_date) from ODS.t1080_tb_gl_bal_eod where bkg_date <= P_TUNGAY - 1)
                 AND ac_no = '103101001'
                 AND branch_code = P_BRANCH
                 AND ac_ccy = P_CCY
                 AND ROWNUM = 1 )
     ,V_TONG_PS AS (
        SELECT SUM(ghi_no) AS tong_ghi_no, SUM(ghi_co) AS tong_ghi_co
                FROM (SELECT external_ref_no AS so_giao_dich,
                             trn_ref_no AS so_but_toan,
               DECODE(drcr_ind,'D',fcy_amount,0) AS ghi_no,
                             DECODE(drcr_ind,'C',fcy_amount,0) AS ghi_co,
                             ac_ccy AS tien_te,
                             SUBSTR(external_ref_no, 1, 3) AS ma_chi_nhanh,
                             user_id
                       FROM hoc_tap_fcc.acvw_all_ac_entries /* vndklb.acvw_all_ac_entries@fcpdb */
                       WHERE ac_no = '103101001'
                         AND ac_ccy = P_CCY
                         AND ac_branch = P_BRANCH
                         AND trn_dt = P_TUNGAY
                         --AND (P_USER is null or user_id = P_USER)
                         AND user_id <> 'SYSTEM'
             ))
select distinct so_du_cuoi into so_du_cuoi_FCC
      From(
   SELECT  --DISTINCT NVL(so_du_dau,0) so_du_dau,
             DISTINCT NVL((SELECT so_du_dau FROM V_SO_DU_DAU),0) so_du_dau,
             SUBSTR(so_giao_dich,1,16) so_giao_dich,
             so_but_toan,
             ghi_no,
             ghi_co,
             tien_te,
             ma_chi_nhanh,
             user_id,

       NVL((SELECT tong_ghi_no FROM V_TONG_PS),0) tong_ghi_no,
       NVL((SELECT tong_ghi_co FROM V_TONG_PS),0) tong_ghi_co,
       NVL((SELECT so_du_dau FROM V_SO_DU_DAU),0) + NVL((SELECT tong_ghi_no FROM V_TONG_PS),0) - NVL((SELECT tong_ghi_co FROM V_TONG_PS),0) so_du_cuoi,
       FUNCTION_CODE_DESC Description
      FROM (
       SELECT  so_giao_dich,
                     so_but_toan,
                     ghi_no,
                     ghi_co,
                     tien_te,
                     ma_chi_nhanh,
                     A.user_id,
           C.FUNCTION_CODE_DESC
                FROM (
            SELECT so_giao_dich,
                   so_but_toan,
                   SUM(ghi_no) ghi_no,
                 SUM(ghi_co) ghi_co,
                 tien_te,
                 ma_chi_nhanh,
                 user_id
            FROM (
                  SELECT    CASE WHEN A.EVENT = 'DEBK' THEN C.TXN_REF_NO
                          WHEN A.EVENT = 'DTOP' THEN D.TXN_REF_NO
                          WHEN A.EVENT = 'INIT' AND A.TRN_CODE = 'CGC' THEN E.TXN_REF_NO
                          ELSE external_ref_no
                       END AS so_giao_dich,
                       trn_ref_no AS so_but_toan,
                       DECODE(drcr_ind,'D',fcy_amount,0) AS ghi_no,
                       DECODE(drcr_ind,'C',fcy_amount,0) AS ghi_co,
                       ac_ccy AS tien_te,
                       SUBSTR(external_ref_no, 1, 3) AS ma_chi_nhanh,
                       A.user_id
                  FROM hoc_tap_fcc.acvw_all_ac_entries A /* vndklb.acvw_all_ac_entries@fcpdb */
                  LEFT JOIN hoc_tap_fcc.SRV_TB_CH_TD_ACCOUNT C on a.RELATED_ACCOUNT = C.td_ac_no /* OBBR_REMO_TDS.SRV_TB_CH_TD_ACCOUNT@fcpdb */

          LEFT JOIN hoc_tap_fcc.SRV_TB_BC_TXN_LOG D /* OBBR_REMO_CMN.SRV_TB_BC_TXN_LOG@fcpdb */
                      ON  A.EXTERNAL_REF_NO = json_value(D.OUTGOING_RES, '$.data.dataPayload.topUpRefNo' RETURNING VARCHAR2(32))
                          And Trunc(D.req_date) = a.trn_dt
                  LEFT JOIN hoc_tap_fcc.SRV_TB_BC_TXN_LOG E /* OBBR_REMO_CMN.SRV_TB_BC_TXN_LOG@fcpdb */
                      ON A.EXTERNAL_REF_NO =  json_value(E.OUTGOING_RES, '$.data.dataPayload.redeemRefNo' RETURNING VARCHAR2(32))
                         AND Trunc(E.req_date) = a.trn_dt

          WHERE ac_no = '103101001'
                      AND ac_ccy = P_CCY
                      AND ac_branch = P_BRANCH
                      AND trn_dt = P_TUNGAY
                     -- AND (P_USER is null or A.user_id = P_USER)
                      AND A.user_id <> 'SYSTEM'
                )
                GROUP BY so_giao_dich,
                     so_but_toan,
                     tien_te,
                     ma_chi_nhanh,
                     user_id
            ) A
        --VINHNH20240731 ITS_113767 LEFT JOIN hoc_tap_fcc.SRV_TB_AD_CENTRAL_TXN_LOG B ON SUBSTR(A.so_giao_dich,1,16) = B.TXN_REF_NO /* OBBR_REMO_ADP.SRV_TB_AD_CENTRAL_TXN_LOG@fcpdb */
    LEFT JOIN hoc_tap_fcc.SRV_TB_BC_EJ_LOG B ON SUBSTR(A.so_giao_dich,1,16) = B.TXN_REF_NO --VINHNH20240731 ITS_113767 /* OBBR_REMO_CMN.SRV_TB_BC_EJ_LOG@fcpdb */
        LEFT JOIN hoc_tap_fcc.SRV_TM_BC_FUNCTION_INDICATOR C ON C.FUNCTION_CODE = B.FUNCTION_CODE /* OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB */
      )
      --RIGHT JOIN V_SO_DU_DAU ON 1=1 -----SONNT1: 98308
      RIGHT JOIN V_TONG_PS ON 1=1-----SONNT1: 98308
       ORDER BY so_giao_dich
     );

  end if;
 --end ECC so du

        OPEN resData for
          select SoDuDau AS SODUDAU,
                 t1080_fn_docso_vn(SoDuDau, P_CCY) as BangChuSoDau,
                 SoDuCuoi AS SODUCUOI,
                 t1080_fn_docso_vn(SoDuCuoi, P_CCY) as BangChuSoCuoi
                 , so_du_cuoi_FCC as SoDuCuoiFCC
                 , SoDuCuoi - so_du_cuoi_FCC as ChenhLech

            FROM dual;
        return resData;
      END;
    ELSE
      BEGIN
        OPEN resData for
          SELECT tnx.TRN_REF_NO as REF_NO,
                 tbf.FUNCTION_CODE_DESC AS ACCOUNT,
                 tnx.checker_id AS CashBox,
                 nvl(tnx.inflow, 0) AS THU,
                 nvl(tnx.outflow, 0) AS CHI
            FROM hoc_tap_fcc_new.Srv_Tb_Ch_Till_Txn tnx /* Obbr_Remo_Cmn.Srv_Tb_Ch_Till_Txn@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
            join (select FUNCTION_CODE, TXN_REF_NO, ACCOUNT_NUMBER
                    from (select FUNCTION_CODE,
                                 TXN_REF_NO,
                                 ACCOUNT_NUMBER,
                                 RANK() OVER(PARTITION BY TXN_REF_NO ORDER BY TXN_TIME_RECEIVED DESC) AS RANK_NUM
                            from hoc_tap_fcc_new.Srv_Tb_Bc_Ej_Log /* Obbr_Remo_Cmn.Srv_Tb_Bc_Ej_Log@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
                           WHERE function_code not in
                                 ('9005', '9006', '9007', '9008'))
                   where RANK_NUM = 1

                  ) tn
              on tnx.TRN_REF_NO = tn.TXN_REF_NO
            join (SELECT FUNCTION_CODE, FUNCTION_CODE_DESC
                    FROM hoc_tap_fcc_new.SRV_TM_BC_FUNCTION_INDICATOR) tbf /* OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM */
              on tbf.function_code = tn.function_code
           where tn.function_code not in ('9005', '9006', '9007', '9008')
             and tnx.CURRENCY_CODE = P_CCY
             and to_char(tnx.branch_date, 'yyyymmdd') =
                 to_char(P_TUNGAY, 'yyyymmdd')
             and (upper(P_REF_NO) = 'ALL' OR tnx.trn_ref_no = P_REF_NO)
             and (instr(it_fn_get_str_br(P_BRANCH), tnx.branch_code) > 0)
           order by tnx.TRN_REF_NO;
        return resData;
      END;
    END IF;
  end NBKNQ_Ver1;
  
  
 /* 
 -- ORG 
  BEGIN
    ---- kiem tra so luong :
    SELECT count(1)
      into v_count
      FROM OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
     WHERE to_char(BRANCH_DATE, 'yyyymmdd') <=
           to_char(P_TUNGAY, 'yyyymmdd')
       and TILL_VAULT_INDICATOR = 'V'
       and (instr(it_fn_get_str_br(P_BRANCH), BRANCH_CODE) > 0)
          --AND BRANCH_CODE = P_BRANCH
       and currency_code = P_CCY;
  
    IF REGEXP_COUNT(P_TYPE, 'INFO') >= 1 THEN
      BEGIN
        ---SoDuDau------------------
        SoDuDau := 0;
        if v_count >= 2 then
          select sum(CURRENT_BALANCE) --add sum vi biet dau n? xuat hien 2 row
            into SoDuDau
            from (SELECT ID,
                         NVL(CURRENT_BALANCE, 0) AS CURRENT_BALANCE,
                         BRANCH_CODE,
                         BRANCH_DATE,
                         ROW_NUMBER() OVER(PARTITION BY BRANCH_CODE ORDER BY BRANCH_DATE desc) row_num
                    FROM OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
                   WHERE to_char(BRANCH_DATE, 'yyyymmdd') <
                         to_char(P_TUNGAY, 'yyyymmdd')
                     and TILL_VAULT_INDICATOR = 'V'
                     and (instr(it_fn_get_str_br(P_BRANCH), BRANCH_CODE) > 0)
                        --AND BRANCH_CODE = P_BRANCH
                     and currency_code = P_CCY)
           where row_num = 1; --truonghm edit 0505 sua 2 thanh 1 va dieu kien <= thanh <
        
        end if;
      
        \*SELECT nvl(OPENING_BALANCE,0) into SoDuDau
        FROM OBBR_REMO_CMN.SRV_TB_CH_TILL_TOT@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
          WHERE to_char(BRANCH_DATE, 'yyyymmdd') =PREDATE
                and teller_id = 'HAVUTT'
                and CURRENCY_CODE = P_CCY; *\
        ---SoDuCuoi------------------
        SoDuCuoi := 0;
        select (SoDuDau + NVL(sum(THU), 0) - NVL(sum(CHI), 0))
          into SoDuCuoi
          FROM (SELECT nvl(tnx.inflow, 0) AS THU, nvl(tnx.outflow, 0) AS CHI
                  FROM Obbr_Remo_Cmn.Srv_Tb_Ch_Till_Txn@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM tnx
                  join (select FUNCTION_CODE, TXN_REF_NO, ACCOUNT_NUMBER
                         from (select FUNCTION_CODE,
                                      TXN_REF_NO,
                                      ACCOUNT_NUMBER,
                                      RANK() OVER(PARTITION BY TXN_REF_NO ORDER BY TXN_TIME_RECEIVED DESC) AS RANK_NUM
                                 from Obbr_Remo_Cmn.Srv_Tb_Bc_Ej_Log@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
                                WHERE function_code not in
                                      ('9005', '9006', '9007', '9008'))
                        where RANK_NUM = 1) tn
                    on tnx.TRN_REF_NO = tn.TXN_REF_NO
                  join (SELECT FUNCTION_CODE, FUNCTION_CODE_DESC
                         FROM OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM) tbf
                    on tbf.function_code = tn.function_code
                 where tn.function_code not in
                       ('9005', '9006', '9007', '9008')
                   and to_char(tnx.branch_date, 'yyyymmdd') =
                       to_char(P_TUNGAY, 'yyyymmdd')
                   and (upper(P_REF_NO) = 'ALL' OR tnx.trn_ref_no = P_REF_NO)
                   and tnx.CURRENCY_CODE = P_CCY
                   and (instr(it_fn_get_str_br(P_BRANCH), tnx.branch_code) > 0));
      
        OPEN resData for
          select SoDuDau AS SODUDAU,
                 t1080_fn_docso_vn(SoDuDau, P_CCY) as BangChuSoDau,
                 SoDuCuoi AS SODUCUOI,
                 t1080_fn_docso_vn(SoDuCuoi, P_CCY) as BangChuSoCuoi
            FROM dual;
        return resData;
      END;
    ELSE
      BEGIN
        OPEN resData for
          SELECT tnx.TRN_REF_NO as REF_NO,
                 tbf.FUNCTION_CODE_DESC AS ACCOUNT,
                 tnx.checker_id AS CashBox,
                 nvl(tnx.inflow, 0) AS THU,
                 nvl(tnx.outflow, 0) AS CHI
            FROM Obbr_Remo_Cmn.Srv_Tb_Ch_Till_Txn@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM tnx
            join (select FUNCTION_CODE, TXN_REF_NO, ACCOUNT_NUMBER
                    from (select FUNCTION_CODE,
                                 TXN_REF_NO,
                                 ACCOUNT_NUMBER,
                                 RANK() OVER(PARTITION BY TXN_REF_NO ORDER BY TXN_TIME_RECEIVED DESC) AS RANK_NUM
                            from Obbr_Remo_Cmn.Srv_Tb_Bc_Ej_Log@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM
                           WHERE function_code not in
                                 ('9005', '9006', '9007', '9008'))
                   where RANK_NUM = 1
                  
                  ) tn
              on tnx.TRN_REF_NO = tn.TXN_REF_NO
            join (SELECT FUNCTION_CODE, FUNCTION_CODE_DESC
                    FROM OBBR_REMO.SRV_TM_BC_FUNCTION_INDICATOR@FCPDB.COREPRODDB.COREPRODVCN.ORACLEVCN.COM) tbf
              on tbf.function_code = tn.function_code
           where tn.function_code not in ('9005', '9006', '9007', '9008')
             and tnx.CURRENCY_CODE = P_CCY
             and to_char(tnx.branch_date, 'yyyymmdd') =
                 to_char(P_TUNGAY, 'yyyymmdd')
             and (upper(P_REF_NO) = 'ALL' OR tnx.trn_ref_no = P_REF_NO)
             and (instr(it_fn_get_str_br(P_BRANCH), tnx.branch_code) > 0)
           order by tnx.TRN_REF_NO;
        return resData;
      END;
    END IF;
  end NBKNQ_Ver1;*/
