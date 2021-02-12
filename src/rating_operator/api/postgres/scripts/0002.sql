CREATE TABLE steps (
    name         VARCHAR(63) PRIMARY KEY,
    sources      TEXT[] NOT NULL,
    labels       JSONB,
    operation    VARCHAR(63) NOT NULL,
    last_updated TIMESTAMP
);