-- Find the top three employees in terms of savings within the job that has
-- the third-highest average salary. List the names of these employees.

with 
    avg_salary as (
        select 
            job, 
            dense_rank() over ( order by avg(salary)) as job_ranking
        from employment
        group by job
    ),
    rank_3 as (
        select job from avg_salary where job_ranking = 3
    ),
    ranked_employees as (
        select 
            finances.parent_id,
            savings,
            dense_rank() over ( order by finances.savings desc) as emp_ranking
        from 
            finances 
        inner join
            employment
        on
            finances.parent_id = employment.parent_id
        inner join
            rank_3
        on
            rank_3.job = employment.job
    )
select 
    -- remp.parent_id,
    first_name, last_name,
    -- employment.job,
    remp.savings
from mailing
inner join ranked_employees as remp
on mailing.parent_id = remp.parent_id
-- inner join employment
-- on remp.parent_id = employment.parent_id
where remp.emp_ranking <= 3;
