import json
import pandas as pd
import sqlite3
import logging
import argparse
import datetime

'''
    Read a json dump of the ROR data and convert it to relational tables in sqlite.
    
    The table structure is similar to the structure provided by Digital Science for the GRID data:
        CREATE TABLE "ror" ( "ror_id" TEXT, "name" TEXT )
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

    The database is written to a file with the same name as the input file with .json replaced by .db,
    for example v1.1-2022-06-16-ror-data.json -> v1.1-2022-06-16-ror-data.db

    Reference: https://towardsdatascience.com/all-pandas-json-normalize-you-should-know-for-flattening-json-13eae1dfb7dd
    '''


def tableCounts(c):
    '''
        Count the number of rows in all tables in a sqlite database
    '''
    c.execute("select name from sqlite_master where type='table'")

    for t in c.fetchall():
        c.execute("select count(*) from {}".format(t[0]))
        lggr.info(f"{t[0]}: {c.fetchone()[0]} rows")


def cleanup(c):
    '''
        cleanup column names
    '''
    cleanup = [ 
        "ALTER TABLE acronyms RENAME COLUMN '0'  TO 'acronym';",
        "ALTER TABLE aliases RENAME COLUMN '0'  TO 'alias';",
        "ALTER TABLE email_address RENAME COLUMN 'id' TO 'ror_id';",
        "ALTER TABLE institutes RENAME COLUMN 'id' TO 'ror_id';",
        "ALTER TABLE links RENAME COLUMN '0'  TO 'link';",
        "ALTER TABLE types RENAME COLUMN '0'  TO 'type';",
    ]

    for s in cleanup:
        c.execute(s)


commandLine = argparse.ArgumentParser(prog='ROR2DB',
                description='convert ROR json data dump to sqlite database'
)
commandLine.add_argument("-if","--inputFile", type=str,
                          help='file with ROR json'
)
commandLine.add_argument('--loglevel', default='info',
                choices=['debug', 'info', 'warning'],
                help='Logging level'
)
commandLine.add_argument('--logto', metavar='FILE',
                    help='Log file (will append to file if exists)'
)

args = commandLine.parse_args()

if args.logto:
    # Log to file
    logging.basicConfig(
        filename=args.logto, filemode='a',
        format='%(asctime)s:%(levelname)s:%(name)s: %(message)s',
        level=args.loglevel.upper(),
        datefmt='%Y-%m-%d %H:%M:%S')
else:
    # Log to stderr
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(name)s: %(message)s',
        level=args.loglevel.upper(),
        datefmt='%Y-%m-%d %H:%M:%S')

lggr = logging.getLogger('ROR2DB')

#
# many of the tables required for the grid database can be created directly from the json with
# a structure of ror and some other simple fields
#
simpleTableFields = {
    'ror': ['id', 'name'],
    'institutes': ['id','name','wikipedia_url','established'],
    'email_address': ['id', 'email_address']
}
#
# list fields are converted to lookup tables with ror_id and value
# 
lookupFields = ['acronyms', 'aliases', 'links', 'relationships', 'addresses', 'labels', 'types']
idFields = ['relationships']
unknowns = ['external_ids']

dataFrames = {}                                         # initialize dataFrame dictionary

with open(args.inputFile, 'r') as f:                    # load input file
    data = json.load(f)
    
lggr.info(f'File {args.inputFile} has {len(data)} records\n')#
data_df = pd.json_normalize(data)                       # normalize the entire json dataset into a single dataframe

lggr.info('Creating dataFrames')

for table in (simpleTableFields):                       # some tables can be created by extracting the correct columns
                                                        # directly from the dataframe (see simpleTableField dictionary)
    fields = simpleTableFields[table]
    dataFrames[table] = data_df[fields]
    lggr.info(f'Dataframe {table} created with {len(dataFrames[table])} rows')
#
# create lookup tables
#
for table in lookupFields:
    dataFrames[table] = pd.json_normalize(
                    data, 
                    record_path =[table], 
                    meta=['id'],
                    meta_prefix = 'ror_'
    )
    lggr.info(f'DataFrame {table} created with {len(dataFrames[table])} rows')
#
# create external_id dataframe
#
ex_id_l = []
for r in data:
    ror_id = r.get('id')
    for k,d in r.get('external_ids').items():           # for each dict in external_ids
        pref_id = d.get('preferred')                    # get preferred id
        all = d.get('all')                              # get list of all ids
        if isinstance(all, str):
            all = [all]

        for i in all:                                       # loop all ids
            ex_id_l.append({'ror_id': ror_id,               # append dict to list
                        'external_type': k,
                        'external_id': i,
                        'preferred': i == pref_id})

dataFrames['external_ids'] = pd.DataFrame(ex_id_l)          # create dataFrame
lggr.info(f'DataFrame external_ids created with {len(dataFrames["external_ids"])} rows')

database = args.inputFile.replace('.json','.db')            # replace .json in inputFile name with .db
con = sqlite3.connect(database)                             # create database connection
cur = con.cursor()                                          # and cursor

for table in list(dataFrames):                              
    dataFrames[table].to_sql(table, con,                    # convert dataframes to tables
                             if_exists='replace',           # in database
                             index=False)                   # do not include index column
        
lggr.info(f'\nDatabase {database} created.')

lggr.info(f'Table counts:')                  # count rows
tableCounts(cur)

cleanup(cur)       # cleanup the column names
con.commit()       # commit database
con.close()        # close database