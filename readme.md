#Pocket_HI
A plsql package to store monthly data, with horizontal and vertical transparent compression.  It is a proof of concept that uses views with triggers on  insert/update/delete ( instead of ). 

----------

##Features
1. Pocket_HI provides a **table of tables** with history. 
2. An unified updatable view of a fast **_flat frontend_** with a **_compressed backend_** storage. 

###The *unified view - VHIMO* 
> The view **VHIMO** is an updatable simultaneous view of the front and the backend data. 
An insert/delete on VHIMO is directed to the backend. An update operation adicionally move the data to the frontend.  

###The _flat frontend_ storage
The table **HIMO** is a _flat frontend_ table without compression and fast CRUD operations. Data on this table takes precedence over the backend.
> - key :  pk1 - name of dataset ;  pk2 - key within dataset  
> - valor_num, valor_str - attributes with large cardinality, for example the names and wages of the employees.  
> - n1, n2, v1, v2, v3, v4 - attributes with low cardinality, for example the department code, ...  
> - D1 - month of reference.  

###The _backend compressed_ store
>After the stabilization of the content of **HIMO**, and when the need for a fast access is gone, the data can be transparently moved to the _backend_ storage:  

> - Table **HIST_1** 
with pk1, pk2  , valor_num, valor_str, a pointer to HIST_2 and the period of validity of the data (begin and end date)  (*Vertical compression* i.e. less records)  
> - Table **HIST_2** is a lookup table to store the attributes n1, n2, v1, v2, v3, v4 (*Horizontal compression* i.e. a pointer replaces six attributtes)  
There is no need to update the backend with sql.  
###Usage example
dataset: **payEveryMonth**, payTo, checkNumber,  amount, emittedDate, adminProcess, cashedDate ... refDate  

``` 
-- Insert into the frontend, update and move to backend:  
insert into himo values ('payEveryMonth','emp007' ,1235.05,'Helder Velez' ,20131002,null,'abc456',  
     null,null,null,to_date('201310','yyyymm'),null);    
... and pay to you  
...   and pay to him...  
... finally my check is cashed:  
update himo set n2=20131015 where pk1='payEveryMonth' and pk2='emp007' and D1=to_date('201310','yyyymm');  
select * from himo where pk1='payEveryMonth' and n2 is null; 
... Selecting from VHIMO is safer because we dont have to know where data resides, but slower. 
select * from Vhimo where pk1='payEveryMonth' and pk2='emp007'; 
... when there is no need of a fast access:  
move2back('payEveryMonth',to_date('201310','yyyymm'),'n2 is not null');  

-- OR a direct insert into the backend Vimo in case of no need for fast accesses nor updates, and small tables.
insert into Vhimo ...

```


###List of objects
> - package KHIMO with commands to move from front to back  
> -Tables : HIMO, HIST_1, HIST_2, HIMO_TEMP, HIST_PIVOT_DATE, HIMO_TEST  
> - Views  : VHIMO, VHIMO_BACK
> - Triggers : ghimo_back_d, ghimo_back_i, ghimo_back_u, ghimo_d, ghimo_i, ghimo_u
> - Sequence : shist 
> - Files : 
> himo_create.sql, himo_data.sql, himo_testes.sql, readme.md







> Written with [StackEdit](http://benweet.github.io/stackedit/).