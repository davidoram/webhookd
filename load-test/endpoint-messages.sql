CREATE TABLE msgs (
    key TEXT PRIMARY KEY,
    topic TEXT NOT NULL,  
    value TEXT NOT NULL,
    headers TEXT,
);