drop table if exists business_facts;
drop table if exists users_facts;
drop table if exists checkins;
drop table if exists geolocation;
drop table if exists tips;
drop table if exists reviews;
drop table if exists business;
drop table if exists users;

create table business
(
    business_id text not null
        constraint business_pk
            primary key,
    address     text,
    categories  text,
    is_open     bigint,
    latitude    double precision,
    longitude   double precision,
    name        text,
    postal_code text,
    "Friday"    text,
    "Monday"    text,
    "Saturday"  text,
    "Sunday"    text,
    "Thursday"  text,
    "Tuesday"   text,
    "Wednesday" text
);

create table geolocation
(
    geolocation_id integer primary key,
    state          text,
    city           text
);

create table business_facts
(
    business_id    text,
    geolocation_id integer,
    year           integer,
    review_count   bigint,
    stars          double precision,
    keywords      text[],
    foreign key (business_id) references business (business_id),
    foreign key (geolocation_id) references geolocation (geolocation_id),
    primary key (business_id, geolocation_id, year)
);

create table checkins
(
    business_id text,
    date        timestamp,
    checkins_count integer,
    foreign key (business_id) references business (business_id),
    primary key (business_id, date)
);

create table users
(
    user_id       text primary key,
    name          text,
    yelping_since timestamp
);

create table reviews
(
    review_id   text primary key,
    user_id     text,
    business_id text,
    date        timestamp,
    stars       integer,
    cool        integer,
    funny       integer,
    useful      integer,
    foreign key (business_id) references business (business_id),
    foreign key (user_id) references users (user_id)
);

create table tips
(
    tip_id           bigint primary key,
    business_id      text,
    compliment_count integer,
    date             timestamp,
    text             text,
    user_id          text,
    foreign key (business_id) references business (business_id),
    foreign key (user_id) references users (user_id)
);

create table users_facts
(
    user_id          text,
    year             integer,
    review_count     bigint,
    average_stars    double precision,
    business_count   bigint,
    reactions_count  bigint,
    compliment_count bigint,
    tip_count        bigint,
    foreign key (user_id) references users (user_id),
    primary key (user_id, year)
);