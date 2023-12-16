-- On average, do the children who live in the same residence as their parents
-- or the children who do not live with their parents earn more money. Again,
-- assume that the child's salary is 30% of what their parents earn.

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
    child_salary as (
        select 
            par.child_id, 
            .3 * (par.p1sal + par.p2sal) / 2 as salary,
            par.same_residence
        from par
    )
select same_residence, avg(salary)
from child_salary
group by same_residence;







