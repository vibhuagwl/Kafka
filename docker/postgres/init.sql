-- Databases for implemented services.
CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE replay;

GRANT ALL PRIVILEGES ON DATABASE orders TO ecommerce;
GRANT ALL PRIVILEGES ON DATABASE inventory TO ecommerce;
GRANT ALL PRIVILEGES ON DATABASE replay TO ecommerce;
