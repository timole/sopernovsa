#!/usr/bin/python

import sys, re, pdb
import logging
import argparse

import numpy as np, pandas as pd
import datetime

from pymongo import MongoClient

userIds = {}
userIdSeq = 200000

appIds = {}
appIdSeq = 300000

def parse_args():
    defaultConnString = "mongodb://localhost:27017/"
    parser = argparse.ArgumentParser(description='SOPERNOVSA')
    parser.add_argument('-c', '--connection' , help='Database connection string url, default: ' + defaultConnString, required = False, default = defaultConnString)
    parser.add_argument('-d', '--db' , help='Database name', required = True)
    parser.add_argument('-o', '--output-file', help='Output CSV file', required = True, default = None)
    parser.add_argument('-oi', '--output-file-ids', help='Output CSV file for ids', required = True, default = None)
    args = vars(parser.parse_args())
    return args

args = parse_args()
connectionString = args['connection']
databaseName = args['db']
out = open(args['output_file'], "w")

outIds = open(args['output_file_ids'], "w")

out.write("datetime;municipalityId;applicationId;userId;role;target;text\n")

client = MongoClient(connectionString)
db = client[databaseName]

applications = db.applications
apps = {}
i = 0
for application in applications.find():
    appId = application["_id"]

    if not appId in appIds.keys():
        appIds[appId] = str(appIdSeq)
        appIdSeq = appIdSeq + 1
        pubAppId = appIds[appId]

    municipalityId = application["municipality"]

    comments = application["comments"]

    for comment in comments:
        try:
            userId = comment['user']['id']

            if not userId in userIds.keys():
                userIds[userId] = str(userIdSeq)
                userIdSeq = userIdSeq + 1
                pubUserId = userIds[userId]

            ts = str(datetime.datetime.fromtimestamp(comment['created'] / 1000))
            text = comment['text']
            if text is not None:
                text = re.sub(';', ':', text)
                text = re.sub('\\n', '\\\\n', text)
            else:
                text = ""

            userRole = comment['user']['role']
            target = comment['target']['type']

            row = ts + ";" + municipalityId + ";" + pubAppId + ";" + pubUserId + ";" + userRole + ";" + target + ";" + text + "\n"
            row = row.encode('utf-8')
            out.write(row)

            if i % 1000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
                i = i + 1
        except:
            sys.stdout.write('e')
            sys.stdout.flush()

outIds.write("applicationId;originalApplicationId\n")
for idKey in appIds.keys():
    id = appIds[idKey]
    if id is None or idKey is None:
        print "Error: None:"
        print("id")
        print(id)
        print("idKey")
        print(idKey)
    else:
        outIds.write(id + ";" + idKey + "\n")
