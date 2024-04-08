# Big-Data-Traineeprogramm Challenge

## Setup
I created a Github repository and cloned it onto my virtual machine
```
git clone https://github.com/Jeahy/tankstellenkoenig.git
```
I installed a virtual environment and activated it, because I will try to solve the tasks with Python scripts
```
virtualenv tkvenv
source tkvenv/bin/activate
```
I created a new database, user and tables on my PostgreSQL server
```
psql -U postgres
CREATE DATABASE tk_db;
CREATE USER tk_user WITH PASSWORD 'tk_password';
CREATE TABLE gas_station_23 (
    id UUID PRIMARY KEY NOT NULL,
    version VARCHAR(10) NOT NULL,
    version_time TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    brand TEXT,
    street TEXT,
    house_number TEXT,
    post_code TEXT,
    place TEXT,
    public_holiday_identifier TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    ot_json TEXT NOT NULL
);
CREATE TABLE
CREATE TABLE gas_station_information_history_23 (
    id SERIAL PRIMARY KEY,
    stid UUID NOT NULL,
    e5 SMALLINT,
    e10 SMALLINT,
    diesel SMALLINT,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    changed SMALLINT
);
```
and the same for the 2020-2023 data

I downloaded the data
```
sudo wget -O /home/pkn/tankstellenkoenig/data/history.dump.gz https://creativecommons.tankerkoenig.de/history/history.dump.gz
```


## Welches ist die südlichste Tankstelle Deutschlands?

## Wie hoch war 2022 der höchste Preis für E10?

## Wo gab es vorgestern den günstigsten Diesel?

## Überlege Dir welche Analysen man mit den Daten noch alles machen könnte? Nenne mindestens zwei Möglichkeiten
Vorhersage Benzinpreise für 2024
Verhältnis Lage (Norden, Süden, Osten, Westen) und Preise
Verhältnis Öffnungszeiten und Preise

