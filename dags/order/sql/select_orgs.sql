select s.source, s.owner,
    (select connection_id from ml.owner_marketplace_connection where source = s.source and owner = s.owner and subject = 'ORDER') as order_connection_id,
    (select connection_id from ml.owner_marketplace_connection where source = s.source and owner = s.owner and subject = 'STOCK') as stock_connection_id
    from
    (select distinct source, owner from ml.owner_marketplace_connection) s
order by s.owner