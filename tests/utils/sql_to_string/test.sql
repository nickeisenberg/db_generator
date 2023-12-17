with 
    n as (
        select * from  testtable
        where name = "nick"
    ), -- here is a comment
    m as (
        select * from testtable
        where name = "matt"
    ) -- here is another comment
select * from n
;
