-- command line client requires this delimiter setting

DROP TABLE IF EXISTS trans;
create table trans (
    id Int primary key auto_increment,
    item varchar(20),
    trans_type Float,  -- 1 for buy and -1 for short
    action Float,  -- 1 for buy and -1 for sell
    no_units Float,
    at_price Float
);
DROP TABLE IF EXISTS inventory;
create table inventory (
    item varchar(20),
    trans_type Float,
    inv Int,
    last_price Float,
    cost_basis Float,
    current_value Float,
    realized_profit Float,
    gain Float,
    PRIMARY KEY(item, trans_type)
);

delimiter |
create trigger
    update_inv
after insert on
    trans
for each row
begin
if (new.trans_type > 0 and new.action > 0) then
        INSERT INTO
            inventory
        values 
            (
                new.item, -- item 
                new.trans_type, -- trans_type 
                new.no_units,  -- inv
                new.at_price,  -- last_price
                new.at_price,  -- cost_basis
                new.at_price * new.no_units,  -- current_value
                0,  -- realized_profit
                0  -- gain
            ) 
        ON DUPLICATE KEY UPDATE
            inv = inv + new.no_units,
            last_price = new.at_price,
            cost_basis = (
                ((inv - new.no_units) * cost_basis) 
                + new.no_units * last_price
            ) / inv,
            current_value = inv * new.at_price,
            realized_profit = realized_profit,
            gain = 100.0 * (
                current_value + realized_profit - (inv * cost_basis)
            ) / (inv * cost_basis);
elseif (new.trans_type > 0 and new.action < 0) then
        UPDATE inventory SET
            inv = inv - new.no_units,
            last_price = new.at_price,
            cost_basis = cost_basis,
            current_value = inv * new.at_price,
            realized_profit = realized_profit + (new.no_units * new.at_price),
            gain = 100.0 * (
                current_value + realized_profit - (inv * cost_basis)
            ) / (inv * cost_basis)
        where trans_type = new.trans_type and item = new.item;
elseif (new.trans_type < 0 and new.action < 0) then
        INSERT INTO
            inventory
        values 
            (
                new.item, -- item 
                new.trans_type, -- trans_type 
                new.no_units * -1.0,  -- inv
                new.at_price,  -- last_price
                new.at_price,  -- cost_basis
                new.at_price * new.no_units * -1.0,  -- current_value
                new.at_price * new.no_units,  -- realized_profit 
                0  -- gain
            ) 
        ON DUPLICATE KEY UPDATE 
            inv = inv - new.no_units,
            last_price = new.at_price,
            cost_basis = (
                (-1.0 * (inv + new.no_units) * cost_basis) 
                + (new.no_units * new.at_price)
            ) / inv * -1.0,
            current_value = inv * new.at_price,
            realized_profit = realized_profit + (new.at_price * new.no_units),
            gain = 100.0 * (realized_profit + current_value) / realized_profit;
elseif (new.trans_type < 0 and new.action > 0) then
        UPDATE inventory SET
            inv = inv + new.no_units,
            last_price = new.at_price,
            cost_basis = cost_basis,
            current_value = inv * new.at_price,
            realized_profit = realized_profit - (new.at_price * new.no_units),
            gain = 100.0 * (realized_profit + current_value) / realized_profit
        where trans_type = new.trans_type and item = new.item;
end if;
end;
|
delimiter ;

-- long testing
insert into trans (item, trans_type, action, no_units, at_price)
values 
    ('car', 1, 1, 5, 10),
    ('car', 1, 1, 5, 20),
    ('car', 1, -1, 5, 15),
    ('car', 1, 1, 5, 10)
;

-- short testing
insert into trans (item, trans_type, action, no_units, at_price)
values 
    ('car', -1, -1, 5, 20),
    ('car', -1, -1, 5, 10),
    ('car', -1, 1, 5, 5)
;

select * from inventory;

select * from trans;
