-- sqlite3  --ascii -init ~/work/sql/init-person-address.ddl
--
CREATE TABLE IF NOT EXISTS addresses(address text);
create table if not exists persons(name text);
CREATE TABLE IF NOT EXISTS associations(id int, name text, persons text);
delete from addresses;
delete from persons;
.mode line
.import './random_addresses_fixed.txt' addresses
.import './random_names_fossbytes.csv' persons
ALTER TABLE addresses ADD COLUMN id integer;
update addresses set id = oid;
ALTER TABLE person ADD COLUMN id integer;
update persons set id = oid;
.mode list
.separator |
.output persons.csv
select name, address from persons p, addresses a where p.id = a.id;
.quit



with p AS
(
    select id, name from
    (
        select id, row_number() over( partition by name order by name asc ) as idx,
        name
        from persons
        where name not in ('Gary Moore', 'Amber Jackson', 'John Smith')
    ) as t
    where idx = 1
    union
    select id, name from persons where name in ('Gary Moore', 'Amber Jackson', 'John Smith')
),
a AS
(
    select p.name, a.address
    from p, addresses a
    where p.id < 1001
      and p.id = a.id
),
b AS
(
    select a.id, a.name, '[' || group_concat('"'||b.name||'"', ',') || ']' as associations
    from p a, p b
    where a.id < 1001
    and b.id = a.id + 100
    group by a.name
    -- having count(a.name) > 1
)
insert into associations
select * from b

select name, address, persons
from associations s, addresses a
where s.id = a.id;

