set echo off
set serveroutput on
drop table himo_test;	
drop table himo_test_total;															
create table himo_test as select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo where  rownum <0;			
alter table HIMO_TEST
  add constraint himo_test_pk primary key (PK1, PK2, D1);
create table himo_test_total as select 1 test_num, '123456789012345678901234567890123456789012345678901234567890' description, 1 must_be, 1 result from dual where rownum <0;
delete from himo where pk1 = 'himo_tests';
delete from hist_2 where p1 in (select p1 from hist_1 where pk1 = 'himo_tests');
delete from hist_1 where pk1 = 'himo_tests';
commit;

 -- insert into the background                 															
 -- a   -- himo 2013-09-01  pk1  pk2  valor_num  valor_str  n1  n2  v1  v2  v3  v4  d1      															
  insert into vhimo select    'himo_tests',  '18938627',  990150602  ,'C201206011150602',  2142  ,null,  'A234',  'R12',  null,  null,  to_date('20130901','yyyymmdd')  from dual;      															
  insert into vhimo select    'himo_tests',  '19788699',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '19788729',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '19801964',  990296667  ,'C20121205296667',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '17080293',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20130901','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '17080308',  990311834  ,'B20111220311834',  1237  ,null,  '15327',  'T06',  null,  null,  to_date('20130901','yyyymmdd')   from dual;      															
  commit;                            															
                              															
   -- himo 2013-10-01  pk1  pk2  valor_num  valor_str  n1  n2  v1  v2  v3  v4  d1      															
  insert into vhimo select    'himo_tests',  '18938627',  990150602  ,'C201206011150602',  2142  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '19788699',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '19788729',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '19801964',  990296667  ,'C20121205296667',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '17080293',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  insert into vhimo select    'himo_tests',  '17080310',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd')   from dual;      															
  commit;                                                                           															
insert into himo_test_total (test_num, description, must_be, result ) values (1,'insert vhimo',12, 99);
update himo_test_total set result = 
       ( 
        select  ( select count(*) from vhimo where pk1='himo_tests')-( select count(*) from himo where pk1='himo_tests') from dual
       ) where test_num = 1;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

 -- inserting in the front															
  insert into himo select    'himo_tests',  '18938627',  990150602  ,'C201206011150602',  2142  ,null,  'A234',  'R12',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19788699',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19788729',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19801964',  990296667  ,'C20121205296667',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '17080293',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '17080308',  990311834  ,'B20111220311834',  1237  ,null,  '15327',  'T06',  null,  null,  to_date('20130901','yyyymmdd'),'N'   from dual;      															
  commit;                            															
                              															
   -- himo 2013-10-01  pk1  pk2  valor_num  valor_str  n1  n2  v1  v2  v3  v4  d1      															
  insert into himo select    'himo_tests',  '18938627',  990150602  ,'C201206011150602',  2142  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19788699',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19788729',  990152056  ,'C20121203152056',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '19801964',  990296667  ,'C20121205296667',  2142  ,null,  'A233',  'R12',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '17080293',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  insert into himo select    'himo_tests',  '17080310',  990311834  ,'B20111220311834',  1237  ,null,  '13405',  'A03',  null,  null,  to_date('20131001','yyyymmdd'),'N'   from dual;      															
  commit;                                  															
                        															
 --------------

insert into himo_test_total (test_num,description, must_be, result ) values (2,'insert himo',12, 99);
update himo_test_total set result = 
       ( 
        select count(*) must_be_12 from himo where pk1='himo_tests'
       ) where test_num = 2;
				
 --select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					
														
															
  -- front and backend are equal	

insert into himo_test_total (test_num,description, must_be, result ) values (3,'front and backend are equal',0, 99);
update himo_test_total set result = 
       ( 
	  select count(*) must_be_0 from (                         															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo	where pk1='himo_tests'														
		)      
       ) where test_num = 3;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					
														
  													
															
 -- move month 2013-10 to backend															
 -- first save the data															
truncate table himo_test; 															
insert into himo_test select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo where pk1='himo_tests';															
 -- move to backend															
exec  khimo.ld_himo_force_phase1('himo_tests',to_date('2013-09-01','yyyy-mm-dd'));															
exec  khimo.ld_himo_phase2_parallel('himo_tests',to_date('2013-09-01','yyyy-mm-dd'),1); 															
exec  khimo.ld_himo_force_phase1('himo_tests',to_date('2013-10-01','yyyy-mm-dd'));															
exec  khimo.ld_himo_phase2_parallel('himo_tests',to_date('2013-10-01','yyyy-mm-dd'),1);		
													
													
 --select count(*) must_be_0 from himo where pk1='himo_tests';															
insert into himo_test_total (test_num,description, must_be, result ) values (4,'himo emptied of month 10, moved to backend',0, 99);
update himo_test_total set result = 
       ( 
		select count(*) must_be_0 from himo where pk1='himo_tests'   
			   ) where test_num = 4;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

 
insert into himo_test_total (test_num,description, must_be, result ) values (5,'vhimo is equal to test data',0, 99);
update himo_test_total set result = 
       ( 
		select count(*) must_be_0 from (       -- chech that the move to the back was good                  															
		  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
		  minus 															
		  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test where pk1='himo_tests')  ) where test_num = 5;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

															
															
update vhimo set d1=to_date('2013-10-01','yyyy-mm-dd')															
       where pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-10-01','yyyy-mm-dd') ;															
update himo_test set d1=to_date('2013-10-01','yyyy-mm-dd')															
       where pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-10-01','yyyy-mm-dd') ;	

insert into himo_test_total (test_num,description, must_be, result ) values (6,'vhimo d1 update is equal to test data',0, 99);
update himo_test_total set result = 
       ( 
	select count(*) must_be_0 from (       -- chech that the vhimo and test are equal		
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test )
		  ) where test_num = 6;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

insert into himo_test_total (test_num,description, must_be, result ) values (7,'and himo also has that updated record',1, 99);
update himo_test_total set result = 
       ( 
		select count(*) must_be_1  from himo where pk1='himo_tests'
		  ) where test_num = 7;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					
	   
																												
													
update vhimo set valor_num=12345,valor_str='uhu', n1=99, n2=1955, v1=null,v2='a',v3='a', v4='a'															
       where pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-09-01','yyyy-mm-dd') ;															
update himo_test set valor_num=12345,valor_str='uhu', n1=99, n2=1955, v1=null,v2='a',v3='a', v4='a'															
       where pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-09-01','yyyy-mm-dd') ;	
	   	
insert into himo_test_total (test_num,description, must_be, result ) values (8,'vhimo values updated is equal to test data',0, 99);
update himo_test_total set result = 
       ( 
	select count(*) must_be_0 from (       -- chech that the vhimo and test are equal		
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test )
		  ) where test_num = 8;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

															
  insert into vhimo select    'himo_tests',  '8627',  99015  ,'0602',  null  ,15,  null,  null, 'A234',  'R12',    to_date('20131101','yyyymmdd')   from dual;      															
  insert into himo_test   select    'himo_tests',  '8627',  99015  ,'0602',  null  ,15,  null,  null, 'A234',  'R12',    to_date('20131101','yyyymmdd')  from dual; 					
  
insert into vhimo select    'himo_tests',  '8627',  99015  ,'0602',  null  ,15,  null,  null, 'A234',  'R12',    to_date('20131201','yyyymmdd')   from dual;      															
insert into himo_test   select    'himo_tests',  '8627',  99015  ,'0602',  null  ,15,  null,  null, 'A234',  'R12',    to_date('20131201','yyyymmdd')  from dual; 					
    
  update vhimo set valor_str = 'velez' , n2=20 where pk1 = 'himo_tests' and pk2 = '8627' and d1= to_date('20131101','yyyymmdd');
  update himo_test set valor_str = 'velez' , n2=20 where pk1 = 'himo_tests' and pk2 = '8627' and d1= to_date('20131101','yyyymmdd');
  
  
 insert into himo_test_total (test_num,description, must_be, result ) values (9,'vhimo values updated is equal to test data',0, 99);
update himo_test_total set result = 
       ( 
	select count(*) must_be_0 from (      
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test )
		  ) where test_num = 9;
				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

 															
delete from vhimo where  pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-10-01','yyyy-mm-dd') ;												
delete from himo_test where   pk1='himo_tests' and pk2 in ('18938627') and d1=to_date('2013-10-01','yyyy-mm-dd') ;															

insert into vhimo (pk1,pk2,d1) values ('himo_tests',  'error', to_date('20131201','yyyymmdd'));
insert into vhimo (pk1,pk2,n1,d1) values ('himo_tests',  'error', 55,to_date('20131201','yyyymmdd'));
insert into himo_test (pk1,pk2,d1) values ('himo_tests',  'error', to_date('20131201','yyyymmdd'));
insert into himo_test (pk1,pk2,n1,d1) values ('himo_tests',  'error', 55,to_date('20131201','yyyymmdd'));

insert into himo_test_total (test_num,description, must_be, result ) values (10,'vhimo insert dont accept dup key',0, 99);
update himo_test_total set result = 
       ( 
	select count(*) must_be_0 from (      
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test )
		  ) where test_num = 10;
set echo on 				
 select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;					

 --select * from vhimo where pk1= 'himo_tests' and pk2='error' and d1=to_date('20131201','yyyymmdd');

commit; 

exec  khimo.move2back('himo_tests',to_date('2013-09-01','yyyy-mm-dd'),1);															
exec  khimo.move2back('himo_tests',to_date('2013-11-01','yyyy-mm-dd'),1);															


insert into himo_test_total (test_num,description, must_be, result ) values (11,'after to back',0, 99);
update himo_test_total set result = 
       ( 
	select count(*) must_be_0 from (      
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from vhimo where pk1='himo_tests' 															
	  minus 															
	  select pk1,  pk2,  valor_num,  valor_str,  n1,  n2,  v1,  v2,  v3,  v4,  d1 from himo_test )
		  ) where test_num = 11;
set echo on 				


													
select test_num, description, decode(must_be-result, 0, 'OK','FAIL') RESULT from himo_test_total order by 1;			

drop table himo_test_total;
drop table himo_test;																	
delete from himo where pk1 = 'himo_tests';
delete from hist_2 where p1 in (select p1 from hist_1 where pk1 = 'himo_tests');
delete from hist_1 where pk1 = 'himo_tests';
commit;
																												