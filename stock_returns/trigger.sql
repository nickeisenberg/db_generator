create trigger
    update_inv
after insert on
    transaction_history
for each row
begin
if (new.position_type > 0 and new.action > 0) then
        INSERT INTO
            portfolio
        values 
            (
                new.ticker, 
                new.position_type,
                new.no_shares,
                new.at_price,
                new.at_price,
                new.at_price * new.no_shares,
                0,
                0
            ) 
        ON DUPLICATE KEY UPDATE
            position = position + new.no_shares,
            last_price = new.at_price,
            cost_basis = (
                ((position - new.no_shares) * cost_basis) 
                + new.no_shares * last_price
            ) / position,
            current_value = position * new.at_price,
            realized_profit = realized_profit,
            gain = 100.0 * (
                current_value + realized_profit - (position * cost_basis)
            ) / (position * cost_basis);
elseif (new.position_type > 0 and new.action < 0) then
        UPDATE portfolio SET
            position = position - new.no_shares,
            last_price = new.at_price,
            cost_basis = cost_basis,
            current_value = position * new.at_price,
            realized_profit = realized_profit + (new.no_shares * new.at_price),
            gain = 100.0 * (
                current_value + realized_profit - (position * cost_basis)
            ) / (position * cost_basis)
        where position_type = new.position_type and ticker = new.ticker;
elseif (new.position_type < 0 and new.action < 0) then
        INSERT INTO
            portfolio
        values 
            (
                new.ticker,
                new.position_type,
                new.no_shares * -1.0,
                new.at_price,
                new.at_price,
                new.at_price * new.no_shares * -1.0,
                new.at_price * new.no_shares,
                0
            ) 
        ON DUPLICATE KEY UPDATE 
            position = position - new.no_shares,
            last_price = new.at_price,
            cost_basis = (
                (-1.0 * (position + new.no_shares) * cost_basis) 
                + (new.no_shares * new.at_price)
            ) / position * -1.0,
            current_value = position * new.at_price,
            realized_profit = realized_profit + (new.at_price * new.no_shares),
            gain = 100.0 * (realized_profit + current_value) / realized_profit;
elseif (new.position_type < 0 and new.action > 0) then
        UPDATE portfolio SET
            position = position + new.no_shares,
            last_price = new.at_price,
            cost_basis = cost_basis,
            current_value = position * new.at_price,
            realized_profit = realized_profit - (new.at_price * new.no_shares),
            gain = 100.0 * (realized_profit + current_value) / realized_profit
        where position_type = new.position_type and ticker = new.ticker;
end if;
end;
