-- create a temp table
create external table if not exists jim_bob (
    int_value int,
    string_value varchar(100),
    bool_value boolean
);

-- insert data in to temp table

-- select data from temp table
select count(8) as cnt from jim_bob;

-- delete temp table
drop table if exists jim_bob;
