#!/usr/bin/python

import sys, re, pdb
import logging
import argparse
import json

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
    args = vars(parser.parse_args())
    return args

args = parse_args()
connectionString = args['connection']
databaseName = args['db']
out = open(args['output_file'], "w")

# out.write("applicationId;created;submitted;verdictGiven\n")
out.write("applicationId;operationId;createdDate;submittedDate;verdictGivenDate;canceledDate;isCanceled\n")

client = MongoClient(connectionString)
db = client[databaseName]

applications = db.applications
apps = {}
i = 0
for application in applications.find():
    appId = application["_id"]

    if application["infoRequest"]:
        continue

    print appId
    if not appId in appIds.keys():
        appIds[appId] = str(appIdSeq)
        appIdSeq = appIdSeq + 1
        pubAppId = appIds[appId]

    municipalityId = application["municipality"]

    comments = application["comments"]

#    try:
#    print "created:" + str(application["created"])
#    print "submitted:" + str(application["submitted"])

    created = application["created"]
    if created is not None:
        created = str(datetime.datetime.fromtimestamp(created/1000.0))
    else:
        created = ""

    submitted = application["submitted"]
    if submitted is not None:
        print submitted
        submitted = str(datetime.datetime.fromtimestamp(int(submitted)/1000.0))
    else:
        submitted = ""

    verdictGiven = ""
    verdicts = application["verdicts"]
    for verdict in verdicts:
#        print "verdict:"
#        print verdict
        for paatos in verdict["paatokset"]:
#            print "paatos:"
#            print paatos
            if "paivamaarat" in paatos.keys():
#                print "paivamaarat"
#                print paatos["paivamaarat"]
                if "anto" in paatos["paivamaarat"].keys():
                    pvm = paatos["paivamaarat"]["anto"]

#                    print pvm
                    if pvm is not None and verdictGiven == "":
                        verdictGiven = str(datetime.datetime.fromtimestamp(pvm/1000.0))

#    print "verdictGiven:" + str(verdictGiven)

    if verdictGiven is not None:
        verdictGiven = str(verdictGiven)
    else:
        verdictGiven = ""

    canceled = None
    if "canceled" in application.keys():
        canceled = application["canceled"]

    if canceled is not None:
        print submitted
        canceled = str(datetime.datetime.fromtimestamp(int(canceled)/1000.0))
    else:
        canceled = ""
    isCanceled = "false"
    if canceled != "":
        isCanceled = "true"

    try:
        operationId = application["primaryOperation"]["name"]
    except:
        operationId = ""

    # row = appId + ";" + created + ";" + submitted + ";" + verdictGiven + "\n"
    row = appId + ";" + operationId + ";" + created + ";" + submitted + ";" + verdictGiven + ";" + canceled + ";" + isCanceled + "\n"
    row = row.encode('utf-8')
    print row
    out.write(row)

    if i % 1000 == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
        i = i + 1
#    except:
#        sys.stdout.write('e')
#        sys.stdout.flush()

