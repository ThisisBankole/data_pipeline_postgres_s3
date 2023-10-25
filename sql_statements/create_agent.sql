CREATE TABLE IF NOT EXISTS agents (
        id SERIAL PRIMARY KEY ,
        surname TEXT NOT NULL,
        name TEXT NOT NULL,
        count INTEGER NOT NULL,
        value FLOAT NOT NULL,
        branch_id INTEGER NOT NULL,
        location_id INTEGER NOT NULL,
        FOREIGN KEY(branch_id) REFERENCES branches(id),
        FOREIGN KEY(location_id) REFERENCES locations(id)
        ); 