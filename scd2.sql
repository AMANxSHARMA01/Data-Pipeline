MERGE INTO customer_schema.customer_prod_history AS target
USING customer_schema.customer_staging AS source
ON target.id = source.id AND target.is_current = TRUE

WHEN MATCHED AND (
    target.first_name <> source.first_name OR
    target.last_name <> source.last_name OR
    target.address <> source.address
) THEN
    -- Expire the old record
    UPDATE SET target.end_date = CURRENT_DATE - 1, target.is_current = FALSE

WHEN NOT MATCHED THEN
    -- Insert new record
    INSERT (id, first_name, last_name, address, start_date, end_date, is_current)
    VALUES (source.id, source.first_name, source.last_name, source.address, CURRENT_DATE, '9999-12-31', TRUE);