CREATE TABLE IF NOT EXISTS laptops (
    id SERIAL PRIMARY KEY,
    company VARCHAR(255),
    product VARCHAR(255),
    type_name VARCHAR(255),
    price_euros NUMERIC(10, 2)
);