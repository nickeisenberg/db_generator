-- Suppose that all of the childen who have jobs are earning %30 of what the
-- average of their parents are earning. List the names of the top three children
-- according to their salaries.

with 
    par1 as (
        select 
            child_id, 
            parent1_id, 
            salary as p1sal
        from children as c
        left join employment as e
        on c.parent1_id = e.parent_id
        where is_employed = 1
    ),
    par2 as (
        select 
            child_id, 
            parent2_id, 
            salary as p2sal
        from children as c
        left join employment as e
        on c.parent2_id = e.parent_id
        where is_employed = 1
    ),
    child_salary_rank as (
        select 
            par1.child_id, 
            .3 * (p1sal + p2sal) / 2 as salary,
            dense_rank() over ( order by .3 * (p1sal + p2sal) / 2 desc) as ranking
        from par1
        left join par2
        on par1.child_id = par2.child_id 
    )
select first_name, last_name, cr.salary
from children as c
left join child_salary_rank as cr
on c.child_id = cr.child_id
where cr.ranking <= 3
;

