# ROR to Database (ROR2DB)
The [Research Organizatiopn Registry](https://ror.org) recently took responsibility for providing up-to-date versions of [ROR data in JSON](https://zenodo.org/record/6657125). These data are a great resource and useful for many use cases. There are some use cases where it is convienient to have the ROR data in a relational database. 

This tool converts the ROR data to a relational model implemented in [SQLite](https://www.sqlitetutorial.net/).

# Usage
**Use python ROR2DB.py -h to see this usage description.**

```
usage: ROR2DB [-h] [-if INPUTFILE] [--loglevel {debug,info,warning}] [--logto FILE]

convert ROR json data dump to sqlite database

optional arguments:
  -h, --help            show this help message and exit
  -if INPUTFILE, --inputFile INPUTFILE
                        file with ROR json
  --loglevel {debug,info,warning}
                        Logging level
  --logto FILE          Log file (will append to file if exists)
  
```

# Environment
ROR2DB imports the following python modules:
import json
import pandas as pd
import sqlite3
import logging
import argparse


# Outputs
The data are written to a sqlite file with the same name as the input file with the .json extension replaced by .db.

A database version of the current version v1.1-2022-06-16-ror-data.db can be created using the command: python ROR2DB.py -if v1.1-2022-06-16-ror-data.json. The terminal output is expected to be:

```
2022-06-22 08:15:29:INFO:ROR2DB: File v1.1-2022-06-16-ror-data.json has 102742 records

2022-06-22 08:15:39:INFO:ROR2DB: Creating dataFrames
2022-06-22 08:15:39:INFO:ROR2DB: Dataframe ror created with 102742 rows
2022-06-22 08:15:39:INFO:ROR2DB: Dataframe institutes created with 102742 rows
2022-06-22 08:15:39:INFO:ROR2DB: Dataframe email_address created with 102742 rows
2022-06-22 08:15:40:INFO:ROR2DB: DataFrame acronyms created with 43204 rows
2022-06-22 08:15:50:INFO:ROR2DB: DataFrame addresses created with 102742 rows
2022-06-22 08:15:50:INFO:ROR2DB: DataFrame aliases created with 24447 rows
2022-06-22 08:15:50:INFO:ROR2DB: DataFrame links created with 101208 rows
2022-06-22 08:15:50:INFO:ROR2DB: DataFrame relationships created with 41694 rows
2022-06-22 08:15:50:INFO:ROR2DB: DataFrame labels created with 27354 rows
2022-06-22 08:15:51:INFO:ROR2DB: DataFrame types created with 102712 rows
2022-06-22 08:15:51:INFO:ROR2DB: DataFrame external_ids created with 229705 rows
2022-06-22 08:15:54:INFO:ROR2DB: 
Database v1.1-2022-06-16-ror-data.db created.
2022-06-22 08:15:54:INFO:ROR2DB: Table counts:
2022-06-22 08:15:54:INFO:ROR2DB: ror: 102742 rows
2022-06-22 08:15:54:INFO:ROR2DB: institutes: 102742 rows
2022-06-22 08:15:54:INFO:ROR2DB: email_address: 102742 rows
2022-06-22 08:15:54:INFO:ROR2DB: acronyms: 43204 rows
2022-06-22 08:15:54:INFO:ROR2DB: addresses: 102742 rows
2022-06-22 08:15:54:INFO:ROR2DB: aliases: 24447 rows
2022-06-22 08:15:54:INFO:ROR2DB: links: 101208 rows
2022-06-22 08:15:54:INFO:ROR2DB: relationships: 41694 rows
2022-06-22 08:15:54:INFO:ROR2DB: labels: 27354 rows
2022-06-22 08:15:54:INFO:ROR2DB: types: 102712 rows
2022-06-22 08:15:54:INFO:ROR2DB: external_ids: 229705 rows
```

# Database Structure
The table structure is similar to the structure provided by Digital Science for the GRID data:

```
CREATE TABLE "ror" ( "id" TEXT, "name" TEXT, "country" TEXT, "country_code" TEXT, "status" TEXT )
CREATE TABLE "acronyms" ( "acronym" TEXT, "ror_id" TEXT )
CREATE TABLE "addresses" ( "line" TEXT, "lat" REAL, "lng" REAL, "postcode" TEXT, "primary" INTEGER, 
                           "city" TEXT, "state" TEXT, "state_code" TEXT, "country_geonames_id" INTEGER, 
                           "geonames_city.id" REAL, "geonames_city.city" TEXT, 
                           "geonames_city.nuts_level1.code" TEXT, "geonames_city.nuts_level1.name" TEXT, 
                           "geonames_city.nuts_level2.code" TEXT, "geonames_city.nuts_level2.name" TEXT, 
                           "geonames_city.nuts_level3.code" TEXT, "geonames_city.nuts_level3.name" TEXT, 
                           "geonames_city.geonames_admin1.id" REAL, "geonames_city.geonames_admin1.name" TEXT, 
                           "geonames_city.geonames_admin1.ascii_name" TEXT, 
                           "geonames_city.geonames_admin1.code" TEXT, "geonames_city.geonames_admin2.id" REAL, 
                           "geonames_city.geonames_admin2.name" TEXT, "geonames_city.geonames_admin2.ascii_name" TEXT, 
                           "geonames_city.geonames_admin2.code" TEXT, "geonames_city.license.attribution" TEXT, 
                           "geonames_city.license.license" TEXT, "ror_id" TEXT )
CREATE TABLE "aliases" ( "alias" TEXT, "ror_id" TEXT )
CREATE TABLE "email_address" ( "ror_id" TEXT, "email_address" TEXT )
CREATE TABLE "institutes" ( "ror_id" TEXT, "name" TEXT, "wikipedia_url" TEXT, "established" REAL )
CREATE TABLE "labels" ( "label" TEXT, "iso639" TEXT, "ror_id" TEXT )
CREATE TABLE "links" ( "link" TEXT, "ror_id" TEXT )
CREATE TABLE "relationships" ( "type" TEXT, "label" TEXT, "id" TEXT, "ror_id" TEXT )
CREATE TABLE "types" ( "type" TEXT, "ror_id" TEXT )
```

---
<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png" /></a><br /><span xmlns:dct="http://purl.org/dc/terms/" href="http://purl.org/dc/dcmitype/Text" property="dct:title" rel="dct:type">DataCiteFacets</span> by <a xmlns:cc="http://creativecommons.org/ns#" href="metadatagamechangers.com" property="cc:attributionName" rel="cc:attributionURL">Ted Habermann</a> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/4.0/">Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License</a>.