FUNCTION     T1080_FN_LN44 (p_Branch    Varchar2
                                         ,p_Tungay Date
                                         ,p_Denngay   Date
                                         ,p_So_Tk     Varchar2)
  Return Sys_Refcursor As
  Type t_Cursor Is Ref Cursor;
  v_Cursor t_Cursor;

 begin
if (p_Denngay = trunc(sysdate)) then
Begin
  Open v_Cursor For
   select d.brn,c.branch_code,i.sub_branch_name,to_char(d.effdt,'dd/MM/yyyy') NGAY,c.customer_id MAKH,d.accno, nvl(t.du_cuoi_vnd,0) DUNO, d.CURRSTAT TTN_TRUOC_DC,
    --decode(d.CURRSTAT,'NORM',1,'OD00',1,'OD02',2,'OD03',3,'OD04',4,'OD05',5,'WOFF',6 ) NHOM_NO_TRUOC_DC,
    decode(d.CURRSTAT,'NORM','1','OD00','1','OD02','2','OD03','3','OD04','4','OD05','5','WOFF' ,'XLDPR') NN_TRUOC_DC,
    d.newstat TTN_SAU_DC,
    --decode(d.newstat,'NORM',1,'OD00',1,'OD02',2,'OD03',3,'OD04',4,'OD05',5,'WOFF',6 ) NHOM_NO_SAU_DC,
    decode(d.newstat,'NORM','1','OD00','1','OD02','2','OD03','3','OD04','4','OD05','5','WOFF' ,'XLDPR') NN_SAU_DC,
    d.maker_Id NGUOI_TH
    from hoc_tap_fcc.CLVWS_MANUAL_STATUS_CHANGE d/* from VNDKLB.CLVWS_MANUAL_STATUS_CHANGE@fcpdb */
    inner join hoc_tap_fcc.CLTB_ACCOUNT_APPS_MASTER  c on c.account_number = d.accno /* vndklb.CLTB_ACCOUNT_APPS_MASTER@fcpdb */
    inner join ods_new.it_main_sub i on i.sub_branch = c.branch_code /* ods.it_main_sub */
    left join
    (
      select  h.account_number,sum(amount_due)-sum(amount_settled) du_cuoi_vnd from hoc_tap_fcc.CLTB_ACCOUNT_schedules h /* vndklb.CLTB_ACCOUNT_schedules@fcpdb */
      where component_name = 'PRINCIPAL'
      group by account_number
    ) t on t.account_number =  d.accno
    where  d.effdt between p_Tungay and p_Denngay
    and (p_So_Tk  is null OR p_So_Tk  = 'ALL' OR d.accno = p_So_Tk)
    and (instr(it_fn_get_str_br(P_BRANCH),d.brn) > 0);

  Return v_Cursor;
  END;
else
  Begin
  Open v_Cursor For
   select d.brn,c.branch_code,i.sub_branch_name,to_char(d.effdt,'dd/MM/yyyy') NGAY,c.customer_id MAKH,d.accno, nvl(t.du_cuoi_vnd,0) DUNO, d.CURRSTAT TTN_TRUOC_DC,
    --decode(d.CURRSTAT,'NORM',1,'OD00',1,'OD02',2,'OD03',3,'OD04',4,'OD05',5,'WOFF',6 ) NHOM_NO_TRUOC_DC,
    decode(d.CURRSTAT,'NORM','1','OD00','1','OD02','2','OD03','3','OD04','4','OD05','5','WOFF' ,'XLDPR') NN_TRUOC_DC,
    d.newstat TTN_SAU_DC,
    --decode(d.newstat,'NORM',1,'OD00',1,'OD02',2,'OD03',3,'OD04',4,'OD05',5,'WOFF',6 ) NHOM_NO_SAU_DC,
    decode(d.newstat,'NORM','1','OD00','1','OD02','2','OD03','3','OD04','4','OD05','5','WOFF' ,'XLDPR') NN_SAU_DC,
    d.maker_Id NGUOI_TH
    from hoc_tap_fcc.CLVWS_MANUAL_STATUS_CHANGE d /* VNDKLB.CLVWS_MANUAL_STATUS_CHANGE@fcpdb */
    inner join hoc_tap_fcc.CLTB_ACCOUNT_APPS_MASTER c on c.account_number = d.accno /* vndklb.CLTB_ACCOUNT_APPS_MASTER @fcpdb */
    inner join ods_new.it_main_sub i on i.sub_branch = c.branch_code /* ods.it_main_sub */
    left join demo.tr_ku t on t.so_tk_vay = d.accno and t.transaction_date = p_Denngay  /* tr_ku */
    where  d.effdt between p_Tungay and p_Denngay
    and (p_So_Tk  is null OR p_So_Tk  = 'ALL' OR d.accno = p_So_Tk)
    and (instr(it_fn_get_str_br(P_BRANCH), d.brn) > 0);
  Return v_Cursor;
  END;
END IF;
End T1080_FN_LN44;
