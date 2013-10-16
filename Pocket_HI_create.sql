----------------------------------------------
-- Export file for user POCKET_BI           --
-- Created by Velez on 2013-10-16, 00:12:00 --
----------------------------------------------

spool himo_create.log
set serveroutput on;
set echo on;
drop sequence SHIST;
drop table HIMO_TEMP;
drop table HIST_PIVOT_DATE;
drop table HIMO;
drop table HIST_1;
drop table HIST_2;
drop view VHIMO;
drop package KHIMO;

prompt
prompt Creating table HIMO
prompt ===================
prompt
create table HIMO
(
  PK1       VARCHAR2(20) not null,
  PK2       VARCHAR2(20) not null,
  VALOR_NUM NUMBER(16,2),
  VALOR_STR VARCHAR2(100),
  N1        NUMBER,
  N2        NUMBER,
  V1        VARCHAR2(100),
  V2        VARCHAR2(100),
  V3        VARCHAR2(100),
  V4        VARCHAR2(100),
  D1        DATE not null,
  DIF       VARCHAR2(1)
)
;
alter table HIMO
  add constraint HIMO_PK primary key (PK1, PK2, D1);
create index D1_I on HIMO (D1);
create index N1_I on HIMO (N1);
create index N2_I on HIMO (N2);
create index V1_I on HIMO (V1);
create index V2_I on HIMO (V2);
create index V3_I on HIMO (V3);
create index V4_I on HIMO (V4);

prompt
prompt Creating table HIMO_TEMP
prompt ========================
prompt
create table HIMO_TEMP
(
  X        ROWID,
  ID_TRANS NUMBER default 1
)
;
comment on table HIMO_TEMP
  is 'Used in the VHIMO update, package KHIMO';

prompt
prompt Creating table HIST_PIVOT_DATE
prompt ==============================
prompt
create table HIST_PIVOT_DATE
(
  DX DATE not null
)
;
alter table HIST_PIVOT_DATE
  add constraint PK_HIST_PIVOT_DATE primary key (DX);

prompt
prompt Creating table HIST_1
prompt =====================
prompt
create table HIST_1
(
  PK1       VARCHAR2(20) not null,
  PK2       VARCHAR2(20) not null,
  VALOR_NUM NUMBER(16,2),
  VALOR_STR VARCHAR2(100),
  P1        NUMBER(9),
  DI        DATE not null,
  DF        DATE not null
)
;
alter table HIST_1
  add constraint HIST_1_PK primary key (PK1, PK2, DI, DF);
create index HIST_1_DF_I on HIST_1 (DF);
create index HIST_1_DI_I on HIST_1 (DI);
create index HIST_1_PK_I on HIST_1 (PK1, PK2);
create index HIST_1_P1_I on HIST_1 (P1);

prompt
prompt Creating table HIST_2
prompt =====================
prompt
create table HIST_2
(
  P1 NUMBER(9) not null,
  N1 NUMBER,
  N2 NUMBER,
  V1 VARCHAR2(100),
  V2 VARCHAR2(100),
  V3 VARCHAR2(100),
  V4 VARCHAR2(100)
)
;
alter table HIST_2
  add constraint HIST2_PK primary key (P1);
create index HIST_2_N1_I on HIST_2 (N1);
create index HIST_2_N2_I on HIST_2 (N2);
create index HIST_2_V1_I on HIST_2 (V1);
create index HIST_2_V2_I on HIST_2 (V2);
create index HIST_2_V3_I on HIST_2 (V3);
create index HIST_2_V4_I on HIST_2 (V4);

prompt
prompt Creating sequence SHIST
prompt =======================
prompt
create sequence SHIST
minvalue 0
maxvalue 999999999
start with 226523
increment by 1
cache 20;

prompt
prompt Creating view VHIMO
prompt ===================
prompt
create or replace view vhimo as
select /*+INDEX(h1,HIST_1_PK_I) INDEX_COMBINE */h1.pk1,h1.pk2,h1.valor_num,h1.valor_str, h2.n1 n1 ,h2.n2 n2,
       decode(h2.v1,'~null',null,h2.v1) v1,decode(h2.v2,'~null',null,h2.v2) v2,decode(h2.v3,'~null',null,h2.v3) v3,decode(h2.v4,'~null',null,h2.v4) v4,pd.dx d1
     from HIST_1 h1, HIST_2 h2, HIST_PIVOT_DATE pd
     where 1=1
       and h1.p1 = h2.p1
       and pd.dx between h1.di and h1.df
       and pd.dx not in (select /*+INDEX(h0,HIMO_PK))*/ d1 from HIMO h0 where  h0.pk1=h1.pk1 and h0.pk2=h1.pk2 /*and d1 <> pd.dx */)
union
    select  /*+INDEX(h,HIMO_PK))*/ pk1,pk2,nvl(valor_num,0),nvl(valor_str,''), n1,n2,nvl(v1,''),nvl(v2,''),nvl(v3,''), nvl(v4,''),d1
   from HIMO h;

prompt
prompt Creating view VHIMO_BACK
prompt ========================
prompt
create or replace view vhimo_back as
select /*+INDEX(h1,HIST_1_PK_I) INDEX_COMBINE */h1.pk1,h1.pk2,h1.valor_num,h1.valor_str, nvl(h2.n1,0) n1 ,nvl(h2.n2,0) n2,
       decode(h2.v1,'~null',null,h2.v1) v1,decode(h2.v2,'~null',null,h2.v2) v2,decode(h2.v3,'~null',null,h2.v3) v3,decode(h2.v4,'~null',null,h2.v4) v4,pd.dx d1
     from HIST_1 h1, HIST_2 h2, HIST_PIVOT_DATE pd
     where 1=1
       and h1.p1 = h2.p1
       and pd.dx between h1.di and h1.df;

prompt
prompt Creating package KHIMO
prompt ======================
prompt
create or replace package KHIMO is

  -- Author  : Helder Velez
  -- Created : 2010-07-08 12:44:32
  -- Purpose : HIMO related procs

  --  
  procedure move2back(p_pk1         IN varchar2,
                      p_data        IN date,
                      p_em_paralelo number default 1);

  procedure ld_himo_force_phase1(p_pk1 IN varchar2, p_data IN date); --  pode correr + de 1 vez até completar a VIEW_1
  ---------------------------------------------------------------------------------------------
  -- para cada um que tem dif = 'S'
  --  insere em vhimo_back
  --  delete de HIMO
  procedure ld_himo_phase2_parallel(p_pk1         IN varchar2,
                                    p_data        IN date,
                                    p_em_paralelo number default 1);
  --  pode correr + de 1 vez até completar o tratamento e mesmo em simultâneo com phase1
---------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------

-- housekeeping
--

end KHIMO;
/

prompt
prompt Creating package body KHIMO
prompt ===========================
prompt
create or replace package body KHIMO is

  ----------------------------------------------------------------------
  ----------------------------------------------------------------------
  --   tratamentos com o VHIMO (HIST_1 e HIST2 e HIMO)
  ----------------------------------------------------------------------
  ----------------------------------------------------------------------
  procedure move2back(p_pk1         IN varchar2,
                      p_data        IN date,
                      p_em_paralelo number default 1) is
  begin
    ld_himo_force_phase1(p_pk1, p_data);
    ld_himo_phase2_parallel(p_pk1, p_data, p_em_paralelo);
  end;

  ------------------------------------------------------------------------
  procedure ld_himo_force_phase1(p_pk1 IN varchar2, p_data date) is
    i      number := 0;
    v_data date := p_data;
    cursor c_himo_back(p_pk1 varchar2, v_data date) is
      select h1.rowid rid,
             h1.pk1,
             h1.pk2,
             h1.valor_num,
             h1.valor_str,
             h1.p1,
             h1.df,
             h2.n1,
             h2.n2,
             h2.v1,
             h2.v2,
             h2.v3,
             h2.v4
        from HIST_1 h1, HIST_2 h2
       where h1.p1 = h2.p1
         and h1.pk1 = p_pk1
         and h1.df = add_months(v_data, -1) -- traz o mes anterior em data-fim!!
       order by pk2;
  
    v_himo_back c_himo_back%rowtype;
    cursor c_himo_front(p_pk1 varchar2, v_data date) is
      select rowid rid, t.*
        from HIMO t
       where (t.dif <> 'S' or t.dif is null)
         and t.pk1 = p_pk1
         and t.d1 = v_data
       order by pk2;
    v_himo_front c_himo_front%rowtype;
  
  begin
  
    if v_data is null then
      v_data := trunc(sysdate, 'MM');
    end if;
    --    1ª parte - para cada new_data actualizar dif para os casos que forem diferentes
    --    vai-se fazer com batimento entre ficheiros (lead new_data)
  
    open c_himo_back(p_pk1, v_data);
    fetch c_himo_back
      into v_himo_back;
  
    for v_himo_front in c_himo_front(p_pk1, v_data) loop
      -- posicionar himo_back
      while v_himo_back.pk2 < v_himo_front.pk2 loop
        fetch c_himo_back
          into v_himo_back;
        exit when c_himo_back%NOTFOUND;
      end loop;
      if (c_himo_back%NOTFOUND) or
         (c_himo_back%FOUND and
         NOT (v_himo_back.pk2 = v_himo_front.pk2 and
          v_himo_back.valor_num = v_himo_front.valor_num and
          v_himo_back.valor_str = v_himo_front.valor_str and
          v_himo_back.n1 = v_himo_front.n1 and
          v_himo_back.n2 = v_himo_front.n2 and
          v_himo_back.v1 = v_himo_front.v1 and
          v_himo_back.v2 = v_himo_front.v2 and
          v_himo_back.v3 = v_himo_front.v3 and
          v_himo_back.v4 = v_himo_front.v4 and
          v_himo_back.pk1 = v_himo_front.pk1)) then
        update HIMO x
           set x.dif = 'S' --- marca para tratar na VIEW_2
         where rowid = v_himo_front.rid;
      else
        update HIST_1 h1 set h1.df = v_data where rowid = v_himo_back.rid; -- actualiza _back
        delete from HIMO x -- apaga de front
         where rowid = v_himo_front.rid;
      end if;
    
      i := i + 1;
      if i = 1000 then
        commit;
        i := 0;
      end if;
    end loop;
    commit;
    close c_himo_back;
  
    execute immediate 'analyze table HIMO compute statistics';
    execute immediate 'analyze index himo_pk compute statistics';
  
    dbms_output.put_line('VIEW_1 completa');
  end;
  ------------------------------------------------------------------------
  -------------------------------------------------------
  -- para cada um que tem dif = 'S'
  --
  --  insere em vhimo_back
  --  delete de HIMO
  -- nota demorou 9500s(=160m=2.6h) para mover 370000 registos com parallel = 5 -- taxa ( 39 reg/s)
  --
  procedure ld_himo_phase2_parallel(p_pk1         IN varchar2,
                                    p_data        IN date,
                                    p_em_paralelo number default 1) is
    v_data date := p_data;
    i      number := 1;
  
    em_paralelo varchar2(20) := '(degree ' || to_char(p_em_paralelo) || ')';
  begin
    if v_data is null then
      v_data := trunc(sysdate, 'MM');
    
    end if;
    -- em paralelo
  
    execute immediate 'alter session enable parallel DML';
  
    execute immediate 'alter table HIMO    parallel ' || em_paralelo;
    execute immediate 'alter index himo_pk  parallel ' || em_paralelo;
    --
    execute immediate 'alter table HIST_1 parallel ' || em_paralelo;
    execute immediate 'alter index hist_1_pk_i parallel ' || em_paralelo;
    execute immediate 'alter index HIST_1_DF_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_1_DI_I parallel ' || em_paralelo;
    execute immediate 'alter index hist_1_p1_i parallel ' || em_paralelo;
    --
    execute immediate 'alter table HIST_2 parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_n1_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_n2_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_v1_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_v2_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_v3_I parallel ' || em_paralelo;
    execute immediate 'alter index HIST_2_v4_I parallel ' || em_paralelo;
    --
    execute immediate 'alter table HIST_pivot_date parallel ' ||
                      em_paralelo;
    execute immediate 'alter index pk_HIST_pivot_date  parallel ' ||
                      em_paralelo;
    --
    execute immediate 'alter table HIMO_temp parallel ' || em_paralelo;
  
    -- fim em paralelo
  
    -- REPENSAR (não dá em paralelo) apaga de vhimo  -- por segurança
  
    -- insere rowis em HIMO_temp
    loop
      select SHIST.NEXTVAL into i from dual;
      insert into HIMO_temp
        select t.rowid, i
          from HIMO t
         where rownum < 100000
           and dif = 'S'
           and t.pk1 = p_pk1
           and t.d1 = v_data;
      exit when sql%rowcount = 0;
      commit;
      -- insere em vhimo_back
      insert into VHIMO_back
        select pk1, pk2, valor_num, valor_str, n1, n2, v1, v2, v3, v4, d1
          from HIMO t
         where rowid in (select x from HIMO_temp where id_trans = i);
      -- delete de front
      delete from HIMO t
       where t.rowid in (select x from HIMO_temp where id_trans = i);
      dbms_output.put_line(to_char(sql%rowcount) ||
                           ' records''moved to backend.');
      commit;
      delete from HIMO_temp where id_trans = i;
      commit;
    end loop;
  
    execute immediate 'alter table HIMO parallel (degree 1)';
    execute immediate 'alter index himo_pk parallel (degree 1)';
  
    execute immediate 'alter table HIST_1 parallel (degree 1)';
    execute immediate 'alter table HIST_2 parallel (degree 1)';
    execute immediate 'alter table HIST_pivot_date parallel (degree 1)';
  
    execute immediate 'alter index hist_1_pk_i parallel (degree 1)';
    execute immediate 'alter index HIST_1_DF_I parallel (degree 1)';
    execute immediate 'alter index HIST_1_DI_I parallel (degree 1)';
    execute immediate 'alter index hist_1_p1_i parallel (degree 1)';
  
    execute immediate 'alter index HIST_2_n1_I parallel (degree 1)';
    execute immediate 'alter index HIST_2_n2_I parallel (degree 1)';
    execute immediate 'alter index HIST_2_v1_I parallel (degree 1)';
    execute immediate 'alter index HIST_2_v2_I parallel (degree 1)';
    execute immediate 'alter index HIST_2_v3_I parallel (degree 1)';
    execute immediate 'alter index HIST_2_v4_I parallel (degree 1)';
    -- restaurar indices e analyzes
  
  end;
  -------------------------------------------------------
begin
  -- Initialization
  null;
end KHIMO;
/

prompt
prompt Creating trigger GHIMO_BACK_D
prompt =============================
prompt
create or replace trigger GHIMO_back_D
  instead of delete on VHIMO_back
  for each row
declare
  V_P1           NUMBER;
  v_hist_1_rowid rowid;
  v_di           date;
  v_df           date;
begin
  V_P1           := null;
  v_hist_1_rowid := null;
  v_di           := null;
  v_df           := null;
  begin
    select max(p1)
      into v_p1
      from HIST_2
     where nvl(n1, 0) = nvl(:OLD.n1, 0)
       and nvl(n2, 0) = nvl(:OLD.n2, 0)
       and v1 = nvl(:OLD.v1, '~null')
       and v2 = nvl(:OLD.v2, '~null')
       and v3 = nvl(:OLD.v3, '~null')
       and v4 = nvl(:OLD.v4, '~null')
       and rownum < 2;
  exception
    when others then
      goto fim;
  end;

  begin
    select rowid, di, df
      into v_hist_1_rowid, v_di, v_df
      from HIST_1 h1
     where h1.pk1 = :OLD.pk1
       and h1.pk2 = :OLD.pk2
       and h1.valor_num = :OLD.valor_num
       and h1.valor_str = :OLD.valor_str
       and h1.p1 = v_p1
       and :OLD.d1 BETWEEN h1.di and h1.df
       and rownum < 2;
  exception
    when others then
      null;
  end;
  if v_hist_1_rowid is not null and v_di <> v_df then
    -- tem um registo encostado
    if v_di = :OLD.d1 then
      --    em di
      update HIST_1
         set di = add_months(:OLD.d1, +1)
       where rowid = v_hist_1_rowid;
    
    else
      if v_df = :OLD.d1 then
        -- em df
        update HIST_1
           set df = add_months(:OLD.d1, -1)
         where rowid = v_hist_1_rowid;
      
      else
        -- partir um registo em dois (1 update + 1 insert)
        update HIST_1
           set df = add_months(:OLD.d1, -1)
         where rowid = v_hist_1_rowid;
      
        insert into HIST_1
          (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
        values
          (:OLD.pk1,
           :OLD.pk2,
           :OLD.valor_num,
           :OLD.valor_str,
           v_p1,
           add_months(:OLD.d1, +1),
           v_df);
      
      end if;
    end if;
  else
    -- delete "normal"
    delete from HIST_1 h1
     where h1.pk1 = :OLD.pk1
       and h1.pk2 = :OLD.pk2
       and h1.valor_num = :OLD.valor_num
       and h1.valor_str = :OLD.valor_str
       and h1.p1 = v_p1;
  end if;

  <<fim>>
  null;
exception
  when others then
    null;
end;
/

prompt
prompt Creating trigger GHIMO_BACK_I
prompt =============================
prompt
create or replace trigger GHIMO_back_I
  instead of INSERT on VHIMO_back
  for each row
declare
  V_P1           NUMBER;
  v_hist_1_rowid rowid;
begin
  V_P1           := null;
  v_hist_1_rowid := null;
  begin
    select max(p1)
      into v_p1
      from HIST_2
     where nvl(n1, 0) = nvl(:NEW.n1, 0)
       and nvl(n2, 0) = nvl(:NEW.n2, 0)
       and v1 = nvl(:NEW.v1, '~null')
       and v2 = nvl(:NEW.v2, '~null')
       and v3 = nvl(:NEW.v3, '~null')
       and v4 = nvl(:NEW.v4, '~null')
       and rownum < 2;
  exception
    when others then
      --    dbms_output.put_line('erro' || :NEW.pk1 || :NEW.pk2);
      v_p1 := null;
  end;
  if v_p1 is null then
    SELECT SHIST.NEXTVAL INTO v_p1 FROM DUAL;
    insert into HIST_2
      (p1, n1, n2, v1, v2, v3, v4)
    values
      (v_p1,
       :NEW.n1, --nvl(:NEW.n1, 0),
       :NEW.n2, --nvl(:NEW.n2, 0),
       nvl(:NEW.v1, '~null'),
       nvl(:NEW.v2, '~null'),
       nvl(:NEW.v3, '~null'),
       nvl(:NEW.v4, '~null'));
    insert into HIST_1
      (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
    values
      (:NEW.pk1,
       :NEW.pk2,
       :NEW.valor_num,
       :NEW.valor_str,
       v_p1,
       :NEW.d1,
       :NEW.d1);
  else
    select max(rowid)
      into v_hist_1_rowid
      from HIST_1 h1
     where h1.pk1 = :NEW.pk1
       and h1.pk2 = :NEW.pk2
       and h1.valor_num = :NEW.valor_num
       and h1.valor_str = :NEW.valor_str
       and h1.p1 = v_p1
       and :NEW.d1 BETWEEN h1.di and add_months(h1.df, +1)
       and rownum < 2;
    if v_hist_1_rowid is not null then
      -- tem um registo encostado
      update HIST_1 set df = :NEW.d1 where rowid = v_hist_1_rowid;
    else
      -- insert normal
      insert into HIST_1
        (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
      values
        (:NEW.pk1,
         :NEW.pk2,
         :NEW.valor_num,
         :NEW.valor_str,
         v_p1,
         :NEW.d1,
         :NEW.d1);
    end if;
  
  end if;

exception
  when others then
    --    dbms_output.put_line('erro' || :NEW.pk1 || :NEW.pk2);
    null;
end;
/

prompt
prompt Creating trigger GHIMO_BACK_U
prompt =============================
prompt
create or replace trigger GHIMO_back_U
  instead of update on vhimo_back
  for each row
declare
  v_ho vhimo%rowtype;
  v_hn vhimo%rowtype;
  ---
begin

  v_ho.pk1       := :OLD.pk1;
  v_ho.pk2       := :OLD.pk2;
  v_ho.valor_num := :OLD.valor_num;
  v_ho.valor_str := :OLD.valor_str;
  v_ho.n1        := :OLD.n1;
  v_ho.n2        := :OLD.n2;
  v_ho.v1        := :OLD.v1;
  v_ho.v2        := :OLD.v2;
  v_ho.v3        := :OLD.v3;
  v_ho.v4        := :OLD.v4;
  v_ho.d1        := :OLD.d1;
  v_hn.pk1       := :NEW.pk1;
  v_hn.pk2       := :NEW.pk2;
  v_hn.valor_num := :NEW.valor_num;
  v_hn.valor_str := :NEW.valor_str;
  v_hn.n1        := :NEW.n1;
  v_hn.n2        := :NEW.n2;
  v_hn.v1        := :NEW.v1;
  v_hn.v2        := :NEW.v2;
  v_hn.v3        := :NEW.v3;
  v_hn.v4        := :NEW.v4;
  v_hn.d1        := :NEW.d1;

  begin
    -- n’o liga aos dados de front
    -- update = delete seguido de insert em back
    delete from vhimo_back
     where pk1 = v_ho.pk1
       and pk2 = v_ho.pk2
       and ((v_ho.valor_num is null) OR
           (v_ho.valor_num is not null and valor_num = v_ho.valor_num))
       and ((v_ho.valor_str is null) OR
           (v_ho.valor_str is not null and valor_str = v_ho.valor_str))
       and ((v_ho.n1 is null) OR (v_ho.n1 is not null and n1 = v_ho.n1))
       and ((v_ho.n2 is null) OR (v_ho.n2 is not null and n2 = v_ho.n2))
       and ((v_ho.v1 is null) OR (v_ho.v1 is not null and v1 = v_ho.v1))
       and ((v_ho.v2 is null) OR (v_ho.v2 is not null and v2 = v_ho.v2))
       and ((v_ho.v3 is null) OR (v_ho.v3 is not null and v3 = v_ho.v3))
       and ((v_ho.v4 is null) OR (v_ho.v4 is not null and v4 = v_ho.v4))
       and d1 = v_ho.d1;
    insert into vHIMO_back -- insere em front j  com os novos valores
      (pk1, pk2, valor_num, valor_str, n1, n2, v1, v2, v3, v4, d1)
    values
      (v_hn.pk1,
       v_hn.pk2,
       v_hn.valor_num,
       v_hn.valor_str,
       v_hn.n1,
       v_hn.n2,
       v_hn.v1,
       v_hn.v2,
       v_hn.v3,
       v_hn.v4,
       v_hn.d1);
  exception
    when others THEN
      NULL;
  end;

end;
/

prompt
prompt Creating trigger GHIMO_D
prompt ========================
prompt
create or replace trigger GHIMO_D
  instead of delete on VHIMO
  for each row
declare
  V_P1           NUMBER;
  v_hist_1_rowid rowid;
  v_di           date;
  v_df           date;
begin
  V_P1           := null;
  v_hist_1_rowid := null;
  v_di           := null;
  v_df           := null;

  begin
    -- apagar do front
    delete from himo h
     where h.pk1 = :OLD.pk1
       and h.pk2 = :OLD.pk2
       and h.valor_num = :OLD.valor_num
       and h.valor_str = :OLD.valor_str;
  exception
    when others then
      null; -- n’o est  no front
  end;

  -- apagar do back
  begin
    select max(p1)
      into v_p1
      from HIST_2
     where nvl(n1, 0) = nvl(:OLD.n1, 0)
       and nvl(n2, 0) = nvl(:OLD.n2, 0)
       and v1 = nvl(:OLD.v1, '~null')
       and v2 = nvl(:OLD.v2, '~null')
       and v3 = nvl(:OLD.v3, '~null')
       and v4 = nvl(:OLD.v4, '~null')
       and rownum < 2;
  exception
    when others then
      goto fim;
  end;

  begin
    select rowid, di, df
      into v_hist_1_rowid, v_di, v_df
      from HIST_1 h1
     where h1.pk1 = :OLD.pk1
       and h1.pk2 = :OLD.pk2
       and h1.valor_num = :OLD.valor_num
       and h1.valor_str = :OLD.valor_str
       and h1.p1 = v_p1
       and :OLD.d1 BETWEEN h1.di and h1.df
       and rownum < 2;
  exception
    when others then
      null;
  end;
  if v_hist_1_rowid is not null and v_di <> v_df then
    -- tem um registo encostado
    if v_di = :OLD.d1 then
      --    em di
      update HIST_1
         set di = add_months(:OLD.d1, +1)
       where rowid = v_hist_1_rowid;
    
    else
      if v_df = :OLD.d1 then
        -- em df
        update HIST_1
           set df = add_months(:OLD.d1, -1)
         where rowid = v_hist_1_rowid;
      
      else
        -- partir um registo em dois (1 update + 1 insert)
        update HIST_1
           set df = add_months(:OLD.d1, -1)
         where rowid = v_hist_1_rowid;
      
        insert into HIST_1
          (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
        values
          (:OLD.pk1,
           :OLD.pk2,
           :OLD.valor_num,
           :OLD.valor_str,
           v_p1,
           add_months(:OLD.d1, +1),
           v_df);
      
      end if;
    end if;
  else
  
    -- delete "normal"
    delete from HIST_1 h1
     where h1.pk1 = :OLD.pk1
       and h1.pk2 = :OLD.pk2
       and h1.valor_num = :OLD.valor_num
       and h1.valor_str = :OLD.valor_str
       and h1.p1 = v_p1;
  
  end if;

  <<fim>>
  null;
exception
  when others then
    null;
  
end GHIMO_D;
/

prompt
prompt Creating trigger GHIMO_I
prompt ========================
prompt
create or replace trigger GHIMO_I
  instead of INSERT on VHIMO
  for each row
declare
  V_P1           NUMBER;
  v_hist_1_rowid rowid;

begin
  V_P1           := null;
  v_hist_1_rowid := null;
  begin
    select max(p1)
      into v_p1
      from HIST_2
     where nvl(n1, 0) = nvl(:NEW.n1, 0)
       and nvl(n2, 0) = nvl(:NEW.n2, 0)
       and v1 = nvl(:NEW.v1, '~null')
       and v2 = nvl(:NEW.v2, '~null')
       and v3 = nvl(:NEW.v3, '~null')
       and v4 = nvl(:NEW.v4, '~null')
       and rownum < 2;
  exception
    when others then
      v_p1 := null;
  end;
  if v_p1 is null then
    SELECT SHIST.NEXTVAL INTO v_p1 FROM DUAL;
    insert into HIST_2
      (p1, n1, n2, v1, v2, v3, v4)
    values
      (v_p1,
       :NEW.n1,
       :NEW.n2,
       nvl(:NEW.v1, '~null'),
       nvl(:NEW.v2, '~null'),
       nvl(:NEW.v3, '~null'),
       nvl(:NEW.v4, '~null'));
    insert into HIST_1
      (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
    values
      (:NEW.pk1,
       :NEW.pk2,
       :NEW.valor_num,
       :NEW.valor_str,
       v_p1,
       :NEW.d1,
       :NEW.d1);
  else
    select max(rowid)
      into v_hist_1_rowid
      from HIST_1 h1
     where h1.pk1 = :NEW.pk1
       and h1.pk2 = :NEW.pk2
       and h1.valor_num = :NEW.valor_num
       and h1.valor_str = :NEW.valor_str
       and h1.p1 = v_p1
       and :NEW.d1 BETWEEN h1.di and add_months(h1.df, +1)
       and rownum < 2;
    if v_hist_1_rowid is not null then
      -- tem um registo encostado
      update HIST_1 set df = :NEW.d1 where rowid = v_hist_1_rowid;
    else
      -- insert normal
      insert into HIST_1
        (pk1, pk2, valor_num, valor_str, p1, di, df) -- df = di
      values
        (:NEW.pk1,
         :NEW.pk2,
         :NEW.valor_num,
         :NEW.valor_str,
         v_p1,
         :NEW.d1,
         :NEW.d1);
    end if;
  
  end if;

exception
  when others then
    null;
end GHIMO_I;
/

prompt
prompt Creating trigger GHIMO_U
prompt ========================
prompt
create or replace trigger GHIMO_U
  instead of update on vhimo
  for each row
declare
  v_ho vhimo%rowtype;
  v_hn vhimo%rowtype;
  ---
begin
  begin
    v_ho.pk1       := :OLD.pk1;
    v_ho.pk2       := :OLD.pk2;
    v_ho.valor_num := :OLD.valor_num;
    v_ho.valor_str := :OLD.valor_str;
    v_ho.n1        := :OLD.n1;
    v_ho.n2        := :OLD.n2;
    v_ho.v1        := :OLD.v1;
    v_ho.v2        := :OLD.v2;
    v_ho.v3        := :OLD.v3;
    v_ho.v4        := :OLD.v4;
    v_ho.d1        := :OLD.d1;
    v_hn.pk1       := :NEW.pk1;
    v_hn.pk2       := :NEW.pk2;
    v_hn.valor_num := :NEW.valor_num;
    v_hn.valor_str := :NEW.valor_str;
    v_hn.n1        := :NEW.n1;
    v_hn.n2        := :NEW.n2;
    v_hn.v1        := :NEW.v1;
    v_hn.v2        := :NEW.v2;
    v_hn.v3        := :NEW.v3;
    v_hn.v4        := :NEW.v4;
    v_hn.d1        := :NEW.d1;
  
    -- update do front , se l  estiver
    update HIMO h
       set h.pk1 = v_hn.pk1,
           pk2   = v_hn.pk2,
           n1    = v_hn.n1,
           n2    = v_hn.n2,
           v1    = v_hn.v1,
           v2    = v_hn.v2,
           v3    = v_hn.v3,
           v4    = v_hn.v4,
           d1    = v_hn.d1
     where 1 = 1
       and h.pk1 = v_ho.pk1
       and h.pk2 = v_ho.pk2
       and h.valor_num = v_ho.valor_num
       and h.valor_STR = v_ho.valor_STR
       and h.n1 = v_ho.n1
       and h.n2 = v_ho.n2
       and h.v1 = v_ho.v1
       and h.v2 = v_ho.v2
       and h.v3 = v_ho.v3
       and h.v4 = v_ho.v4
       and h.d1 = v_ho.d1;
  end;
  -- e se n’o est  no front faz inserÎ’o com os novos valores no front

  begin
    -- para o caso em que n’o estava em front
    insert into HIMO -- insere em front j  com os novos valores
      (pk1, pk2, valor_num, valor_str, n1, n2, v1, v2, v3, v4, d1)
    values
      (v_hn.pk1,
       v_hn.pk2,
       v_hn.valor_num,
       v_hn.valor_str,
       v_hn.n1,
       v_hn.n2,
       v_hn.v1,
       v_hn.v2,
       v_hn.v3,
       v_hn.v4,
       v_hn.d1);
    -- e apaga de back
    delete from vhimo_back
     where pk1 = v_ho.pk1
       and d1 = v_ho.d1;
  
  exception
    when others THEN
      NULL;
  end;

end GHIMO_U;
/


spool off
