CREATE TABLE datasets (
        resolution VARCHAR(256) NOT NULL, 
        frequency VARCHAR(256) NOT NULL, 
        product VARCHAR(256) NOT NULL, 
        year VARCHAR(256) NOT NULL, 
        month VARCHAR(256) NOT NULL, 
        day VARCHAR(256) NOT NULL, 
        hour VARCHAR(256) NOT NULL, 
        minute VARCHAR(256) NOT NULL, 
        second VARCHAR(256) NOT NULL, 
        path VARCHAR(8000), 
        PRIMARY KEY (resolution, frequency, product, year, month, day, hour, minute, second)
);
