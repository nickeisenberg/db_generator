-- Within each profession, what is the percentage of people that make less than
-- the average.

with average as (
    select job, avg(salary) as avg_sal, count(job) as job_amt
    from employment
    group by job
)

select emp.job, count(emp.job) / av.job_amt as num_below
from employment as emp
left join average as av
on
emp.job = av.job
where (salary - av.avg_sal) < 0
group by emp.job
;
