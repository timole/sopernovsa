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

def report_error(errorId):
    sys.stdout.write(errorId)
    sys.stdout.flush()

def parse_date(dateString):
    if dateString is None or dateString == 0:
        return ""
    else:
        return str(datetime.datetime.fromtimestamp(dateString/1000.0))

args = parse_args()
connectionString = args['connection']
databaseName = args['db']
out = open(args['output_file'], "w")

# out.write("applicationId;created;submitted;verdictGiven\n")
out.write("applicationId;municipalityId;state;operationId;operationId2;operationId3;allOperations;createdDate;submittedDate;verdictGivenDate;canceledDate;isCanceled\n")

client = MongoClient(connectionString)
db = client[databaseName]

applications = db.applications
apps = {}
i = 0

for application in applications.find():
    appId = application["_id"]

    if application["infoRequest"]:
        continue

    if not appId in appIds.keys():
        appIds[appId] = str(appIdSeq)
        appIdSeq = appIdSeq + 1
        pubAppId = appIds[appId]

    municipalityId = application["municipality"]

    comments = application["comments"]

    created = parse_date(application["created"])

    submitted = parse_date(application["submitted"])

    verdictGiven = ""
    verdicts = application["verdicts"]
    if len(verdicts) > 0:
        for verdict in verdicts:
            for paatos in verdict["paatokset"]:
                if "paivamaarat" in paatos.keys():
                    if "anto" in paatos["paivamaarat"].keys():
                        pvm = paatos["paivamaarat"]["anto"]
                        if pvm is not None and verdictGiven == "":
                            verdictGiven = parse_date(pvm)
        if verdictGiven == "" and "timestamp" in verdicts[0].keys() and verdicts[0]["timestamp"] is not None:
            verdictGiven = parse_date(verdicts[0]["timestamp"])

    if verdictGiven is not None:
        verdictGiven = str(verdictGiven)
    else:
        verdictGiven = ""

    canceled = None
    if "canceled" in application.keys():
        canceled = application["canceled"]

    if canceled is not None:
        canceled = parse_date(canceled)
    else:
        canceled = ""

    isCanceled = "false"
    if canceled != "":
        isCanceled = "true"

    try:
        operationId = application["primaryOperation"]["name"]
    except:
        operationId = ""
        report_error('o')

    operationId2 = ""
    operationId3 = ""
    allOperations = operationId

    try:
        if "secondaryOperations" in application.keys() and len(application["secondaryOperations"]) > 0:
            operationId2 = application["secondaryOperations"][0]["name"]
        if "secondaryOperations" in application.keys() and len(application["secondaryOperations"]) > 1:
            operationId3 = application["secondaryOperations"][1]["name"]

        for secondaryOperation in application["secondaryOperations"]:
            allOperations = allOperations + ","
            allOperations = allOperations + str(secondaryOperation["name"])
    except:
        report_error('2')

    try:
        state = application["state"]
    except:
        state = ""
        report_error('s')

    try:
        municipalityId = application["municipality"]
    except:
        municipalityId = ""
        report_error('m')

    # row = appId + ";" + created + ";" + submitted + ";" + verdictGiven + "\n"

    row = appId + ";" + municipalityId + ";" + state + ";" + operationId + ";" + operationId2 + ";" + operationId3 + ";" + allOperations + ";"+ created + ";" + submitted + ";" + verdictGiven + ";" + canceled + ";" + isCanceled + "\n"
    row = row.encode('utf-8')
#    print row
    out.write(row)

    if i % 1000 == 0:
        sys.stdout.write('.')
        sys.stdout.flush()

    i = i + 1
#    except:
#        sys.stdout.write('e')
#        sys.stdout.flush()

