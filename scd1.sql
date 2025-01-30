MERGE INTO customer_schema.customer_prod AS t
USING customer_schema.customer_staging AS s
ON (t.ID = s.ID)
WHEN MATCHED THEN 
    UPDATE SET  t.FIRST_NAME = s.FIRST_NAME,
                t.LAST_NAME = s.LAST_NAME,
                t.ADDRESS = s.ADDRESS,
                t.LAST_UPDATED = s.LAST_UPDATED
WHEN NOT MATCHED THEN 
    INSERT (id, first_name, last_name, address, last_updated)
    VALUES (s.ID, s.FIRST_NAME,s.LAST_NAME,s.ADDRESS,s.LAST_UPDATED)