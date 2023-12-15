-- Suppose that all of the childen who have jobs are earning %30 of what the
-- average of their parents are earning. List the names of the top three children
-- according to their salaries.

with 
    par as (
        select 
            child_id, 
            parent1_id, 
            e1.salary as p1sal,
            parent2_id, 
            e2.salary as p2sal,
            same_residence
        from children as c
        left join employment as e1
        on c.parent1_id = e1.parent_id
        left join employment as e2
        on c.parent2_id = e2.parent_id
        where is_employed = 1
    ),
    child_salary_rank as (
        select 
            par.child_id, 
            .3 * (p1sal + p2sal) / 2 as salary,
            dense_rank() over ( order by .3 * (p1sal + p2sal) / 2 desc) as ranking
        from par
    )
select first_name, last_name, cr.salary
from children as c
left join child_salary_rank as cr
on c.child_id = cr.child_id
where cr.ranking <= 3
;

