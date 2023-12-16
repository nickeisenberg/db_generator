-- Find the average number of children that employees have within each job.

with 
    par1 as (
        select 
            parent1_id,
            count(parent1_id) as p1c
        from children
        group by parent1_id
    ),
    par2 as ( 
        select 
            parent2_id,
            count(parent2_id) as p2c
        from children
        group by parent2_id
    ),
    number_of_kids as (
        select
            m.parent_id, ifnull(p1c, 0) + ifnull(p2c, 0) as amt
        from 
            mailing as m
        left join 
            par1
        on 
            m.parent_id = par1.parent1_id
        left join 
            par2
        on 
            m.parent_id = par2.parent2_id
    )
select 
    e.job, avg(amt) 
from
    employment as e
left join 
    number_of_kids as n
on
    e.parent_id = n.parent_id
group by e.job
;
