-- name: schema_up
CREATE TABLE users (id SERIAL PRIMARY KEY);

-- name: schema_down
DROP TABLE users;
