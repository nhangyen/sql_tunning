CREATE OR REPLACE function TEST_BK_20260112
return date
AS
v_date date;
BEGIN 
    select sysdate into v_date from dual;
    return v_date;
end;