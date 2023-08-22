-- List the average savings of the parents with no child, one child and more
-- than one child.
with 
    par1c as (
        select
            m.parent_id,
            count(c1.parent1_id) as p1c
        from 
            mailing as m
        left join
            children as c1
        on
            m.parent_id = c1.parent1_id
        group by m.parent_id
    ),
    par2c as (
        select
            m.parent_id,
            count(c2.parent2_id) as p2c
        from 
            mailing as m
        left join
            children as c2
        on
            m.parent_id = c2.parent2_id
        group by m.parent_id
    ),
    parc as (
        select 
            par1c.parent_id, 
            p1c + p2c as no_of_kids
        from par1c
        left join par2c
        on par1c.parent_id = par2c.parent_id
    )
select 
    case 
        when p.no_of_kids = 0 then 0
        when p.no_of_kids = 1 then 1
        else "more than 2 kids"
    end as no_of_kids,
    avg(f.savings)
from
    parc as p
left join
    finances as f 
on 
    p.parent_id = f.parent_id
group by
    case 
        when p.no_of_kids = 0 then 0
        when p.no_of_kids = 1 then 1
        else "more than 2 kids"
    end
;
