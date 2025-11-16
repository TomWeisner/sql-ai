### SQL Backend

Backends encapsulate how we talk to a particular SQL engine (schema discovery, formatting,
query execution, prompt defaults).  
`base.py` defines the protocol; `table.py` holds the shared metadata object used across the app.  
