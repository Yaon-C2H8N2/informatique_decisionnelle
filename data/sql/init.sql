create table business
(
    business_id  text not null
        constraint business_pk
            primary key,
    address      text,
    categories   text,
    is_open      bigint,
    latitude     double precision,
    longitude    double precision,
    name         text,
    postal_code  text,
    review_count bigint,
    stars        double precision,
    "Friday"     text,
    "Monday"     text,
    "Saturday"   text,
    "Sunday"     text,
    "Thursday"   text,
    "Tuesday"    text,
    "Wednesday"  text
);

create table attributes
(
    attribute_id   integer not null
        constraint attributes_pk
            primary key,
    attribute_name text    not null
);

create table geolocation
(
    geolocation_id integer primary key,
    state          text,
    city           text
);

create table business_facts
(
    business_id     text,
    attribute_id    integer,
    geolocation_id  integer,
    attribute_value text,
    foreign key (business_id) references business (business_id),
    foreign key (attribute_id) references attributes (attribute_id),
    foreign key (geolocation_id) references geolocation (geolocation_id),
    primary key (business_id, attribute_id, geolocation_id)
);

create table time
(
    time_id integer primary key,
    datetime timestamp not null
);

create table checkin_facts
(
    business_id text,
    time_id     integer,
    checkins    integer,
    foreign key (business_id) references business (business_id),
    foreign key (time_id) references time (time_id),
    primary key (business_id, time_id)
)