-- Find the top three employees in terms of savings within the job that has
-- the third-highest average salary. List the names of these employees.

with 
    avg_salary as (
        select 
            job, 
            avg(salary) as avg_sal,
            dense_rank() over ( order by avg(salary)) as job_ranking
        from employment
        group by job
    ),
    rank_3 as (
        select job from avg_salary where job_ranking = 3
    ),
    ranked_employees as (
        select 
            parent_id,
            employment.job,
            salary,
            dense_rank() over ( order by employment.salary desc) as emp_ranking
        from 
            employment
        inner join
            rank_3
        on
            employment.job = rank_3.job
    )
select * from ranked_employees where emp_ranking <= 3;
;
