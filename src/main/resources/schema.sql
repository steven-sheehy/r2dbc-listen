drop table if exists customer;
create table if not exists customer (
  id bigint primary key not null,
  first_name varchar(100) not null
);
