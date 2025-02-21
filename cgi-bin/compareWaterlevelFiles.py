#!/usr/bin/env python3
#
###############################################################################
# $Id: compareWaterlevelFiles.py
#
# Project:  compareWaterlevelFiles.py
# Purpose:  Script compares two tab-limited text files for USGS, CDWR, and OWRD
#           waterlevel records.
# 
# Author:   Leonard Orzol <llorzol@usgs.gov>
#
###############################################################################
# Copyright (c) Leonard Orzol <llorzol@usgs.gov>
# 
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
###############################################################################

import os, sys, string, re

import datetime

import json

import argparse

# Set up logging
#
import logging

# -- Set logging file
#
# Create screen handler
#
screen_logger = logging.getLogger()
formatter     = logging.Formatter(fmt='%(message)s')
console       = logging.StreamHandler()
console.setFormatter(formatter)
screen_logger.addHandler(console)
screen_logger.setLevel(logging.INFO)
screen_logger.propagate = False

program         = "USGS OWRD CDWR Waterlevel Comparing Script"
version         = "2.08"
version_date    = "October 10, 2023"
usage_message   = """
Usage: checkSeasons.py
                [--help]
                [--usage]
                [--sites       Name of collection file listing sites]
                [--file1       Name of first waterlevel file listing groundwater measurements [format USGS old groundwater output]
                [--file2       Name of second waterlevel file listing groundwater measurements [format USGS old groundwater output]
                [--logging     Enable summary operational output to the screen]
"""

# =============================================================================
def errorMessage(error_message):

   screen_logger.info(message)

   sys.exit( 1 )

# =============================================================================

def processCollectionSites (keyColumn, columnL, linesL):

   siteInfoD   = {}
   siteCount   = 0

   keyColumn   = keyColumn.lower()

   # Parse for column names [rdb format]
   #
   while 1:
      if len(linesL) < 1:
         del linesL[0]
      elif linesL[0][0] == '#':
         del linesL[0]
      else:
         namesL = linesL[0].lower().split('\t')
         del linesL[0]
         break

   # Format line in header section
   #
   del linesL[0]

   # Check column names
   #
   if keyColumn not in namesL: 
      message = "Missing index column " + keyColumn + " collection file"
      errorMessage(message)

   # Parse data lines
   #
   while len(linesL) > 0:
      
      if len(linesL[0]) < 1:
         del linesL[0]
         continue
      
      if linesL[0][0] == '#':
         del linesL[0]
         continue

      Line    = linesL[0]
      del linesL[0]
      
      valuesL = Line.split('\t')

      indexSite = str(valuesL[ namesL.index(keyColumn) ])

      recordD = {}
   
      for column in columnL:

         if column in columnL:
            indexValue      = valuesL[ namesL.index(column) ]
            recordD[column] = indexValue
         else:
            message  = "Parsing issue for column %s " % column
            message += "Unable to parse %s" % Line
            errorMessage(message)

      dec_lat_va     = recordD['dec_lat_va']
      dec_long_va    = recordD['dec_long_va']
      status         = "Active"
      gw_begin_date  = None
      gw_end_date    = None
      
      siteCount     += 1

      # Check for sites with no valid location
      #
      if len(dec_lat_va) < 1 or len(dec_long_va) < 1:
         continue
   
      if indexSite not in siteInfoD:
         siteInfoD[indexSite] = {}
               
      siteInfoD[indexSite]['site_id']            = recordD['site_id']
      siteInfoD[indexSite]['agency_cd']          = recordD['agency_cd']
      siteInfoD[indexSite]['site_no']            = recordD['site_no']
      siteInfoD[indexSite]['cdwr_id']            = recordD['cdwr_id'].strip()
      siteInfoD[indexSite]['coop_site_no']       = recordD['coop_site_no']
      siteInfoD[indexSite]['state_well_nmbr']    = recordD['state_well_nmbr']
      siteInfoD[indexSite]['dec_lat_va']         = recordD['dec_lat_va']
      siteInfoD[indexSite]['dec_long_va']        = recordD['dec_long_va']
      siteInfoD[indexSite]['station_nm']         = recordD['station_nm']
      siteInfoD[indexSite]['status']             = status
      siteInfoD[indexSite]['gw_begin_date']      = gw_begin_date
      siteInfoD[indexSite]['gw_end_date']        = gw_end_date
      siteInfoD[indexSite]['gw_agency_cd']       = []
      siteInfoD[indexSite]['gw_count']           = 0
      siteInfoD[indexSite]['gw_status']          = 'Inactive'
      siteInfoD[indexSite]['rc_begin_date']      = ""
      siteInfoD[indexSite]['rc_end_date']        = ""
      siteInfoD[indexSite]['rc_agency_cd']       = []
      siteInfoD[indexSite]['rc_status']          = ""
   
   return siteInfoD
# =============================================================================

def processWls (waterlevel_file, keyColumn, myGwFields, linesL):

   serviceL    = []
   recordCount = 0
   message     = ''
   GwInfoD     = {}
   sitesL      = []
   goodSitesL  = []

   keyColumn   = keyColumn.lower()

   usgsRecords = 0
   owrdRecords = 0
   cdwrRecords = 0
   othrRecords = 0

   agencyL     = []

   numYRecords = 0
   numNRecords = 0
   numBRecords = 0

   localDate   = datetime.datetime.now().strftime("%B %d, %Y")
   localYear   = datetime.datetime.now().strftime("%Y")

   # Parse for column names [rdb format]
   #
   while 1:
      if len(linesL) < 1:
         del linesL[0]
      elif linesL[0][0] == '#':
         del linesL[0]
      else:
         namesL = linesL[0].lower().split('\t')
         del linesL[0]
         break

   # Format line in header section
   #
   #del linesL[0]

   # Check column names
   #
   if keyColumn not in namesL:
      message = "Missing index column " + keyColumn + " waterlevel file"
      errorMessage(message)

   # Parse data lines
   #
   while len(linesL) > 0:

      recordCount += 1

      dataL = ['--'] * len(myGwFields)
      
      if len(linesL[0]) < 1:
         del linesL[0]
         continue
      
      if linesL[0][0] == '#':
         del linesL[0]
         continue

      Line    = linesL[0]
      del linesL[0]

      valuesL    = Line.split('\t')
            
      site_id    = str(valuesL[ namesL.index('site_id') ])
         
      # Check if site is already included
      #
      if site_id not in GwInfoD:

         GwInfoD[site_id] = {}

         sitesL.append(site_id)
      
      # Fix records with poor lev_tm, lev_dt_acy_cd, lev_tz_cd due to default lev_tm = 00:00 and 12:00
      #
      lev_tm = str(valuesL[ namesL.index('lev_tm') ]).strip()
      if lev_tm in ['00:00', '12:00']:
         valuesL[ namesL.index('lev_str_dt') ] = valuesL[ namesL.index('lev_dt') ] 
         valuesL[ namesL.index('lev_tm') ]     = ''
         valuesL[ namesL.index('lev_tz_cd') ]  = ''
         
         if valuesL[ namesL.index('lev_dt_acy_cd') ] == 'm':
            valuesL[ namesL.index('lev_dt_acy_cd') ] = 'D'
         
      # Add records
      #
      lev_dtm = valuesL[ namesL.index('lev_dtm') ][:10]
      if lev_dtm not in GwInfoD[site_id]:

         GwInfoD[site_id][lev_dtm] = {}

      for myColumn in myGwFields:
         if myColumn in namesL:
            GwInfoD[site_id][lev_dtm][myColumn] = str(valuesL[ namesL.index(myColumn) ]).strip()
         else:
            GwInfoD[site_id][lev_dtm][myColumn] = ''

      
      # Static measurement
      #
      lev_web_cd    = valuesL[ namesL.index('lev_web_cd') ]
      if str(lev_web_cd) == 'Y':
         goodSitesL.append(site_id)
         numYRecords += 1
      
      # non-static measurement
      #
      else:
         
         numNRecords += 1
      
      # Measuring agencies
      #
      lev_agency_cd    = valuesL[ namesL.index('lev_agency_cd') ]
      if len(lev_agency_cd) < 1:
         lev_agency_cd = 'None'
         
      if lev_agency_cd not in agencyL:
         agencyL.append(lev_agency_cd)
         
      if str(lev_agency_cd) == 'USGS':
         usgsRecords += 1
      elif str(lev_agency_cd) == 'OWRD':
         owrdRecords += 1
      elif str(lev_agency_cd) == 'CDWR':
         cdwrRecords += 1
      else:
         othrRecords += 1
             
   # Print information
   #
   ncol     = 100
   messages = []
   messages.append('\n\n')
   message = "Groundwater Information"
   fmt     = "%" + str(int(ncol/2-len(message)/2)) + "s%s"
   messages.append(fmt % (' ', message))
   messages.append("=" * ncol)
   message = "Recorded on %s" % localDate
   fmt     = "%" + str(int(ncol/2-len(message)/2)) + "s%s"
   messages.append(fmt % (' ', message))
   messages.append("-" * ncol)
   messages.append('\n\n')
   messages.append('\tProcessed periodic measurements in the %s file' % waterlevel_file)
   messages.append('\t%s' % (81 * '-'))
   messages.append('\t%-70s %10d' % ('Number of of sites with periodic measurements', len(sitesL)))
   messages.append('\t%-70s %10d' % ('Number of of static periodic measurements', numYRecords))
   messages.append('\t%-70s %10d' % ('Number of of non-static periodic measurements', numNRecords))
   messages.append('\t%-70s %10d' % ('Number of of periodic measurements in waterlevel file', recordCount))
   messages.append('')
   messages.append('\t%-70s %10d' % ('Number of of USGS periodic measurements in waterlevel file', usgsRecords))
   messages.append('\t%-70s %10d' % ('Number of of OWRD periodic measurements in waterlevel file', owrdRecords))
   messages.append('\t%-70s %10d' % ('Number of of CDWR periodic measurements in waterlevel file', cdwrRecords))
   messages.append('\t%-70s %10d' % ('Number of of Other periodic measurements in waterlevel file', othrRecords))
   messages.append('\t%-70s' % 'Measuring agencies')
   messages.append('\n'.join( list(map(lambda x: '\t%-10s %70s' % (' ', x), agencyL))))
   messages.append('\t%s' % (81 * '-'))
   
   screen_logger.info('\n'.join(messages))
   file_logger.info('\n'.join(messages))

   return GwInfoD
# =============================================================================

# ----------------------------------------------------------------------
# -- Main program
# ----------------------------------------------------------------------

# Initialize arguments
#
loggingLevel     = 30
debugLevel       = None

# Set arguments
#
parser = argparse.ArgumentParser(prog=program)

parser.add_argument("--usage", help="Provide usage",
                    type=str)
 
parser.add_argument("-sites", "--sites",
                    help="Name of collection file listing sites",
                    required = True,
                    type=str)
 
parser.add_argument("-file1", "--file1",
                    help="Name of first waterlevel file listing groundwater measurements [format USGS old groundwater output]",
                    required = True,
                    type=str)
 
parser.add_argument("-file2", "--file2",
                    help="Name of second waterlevel file listing groundwater measurements [format USGS old groundwater output]",
                    required = True,
                    type=str)
 
parser.add_argument("-log", "-logging","--log", "--logging",
                    help="Enable full operational output to the screen",
                    action="store_true")

# Parse arguments
#
args = parser.parse_args()

# Set collection file
#
if args.sites:

   collection_file = args.sites
   if not os.path.isfile(collection_file):
      message  = "File listing sites %s does not exist" % collection_file
      errorMessage(message)

# Set first waterlevel file
#
if args.file1:

   waterlevel1_file = args.file1
   if not os.path.isfile(waterlevel1_file):
      message  = "Name of first waterlevel file listing groundwater measurements %s does not exist" % waterlevel1_file
      errorMessage(message)

# Set second waterlevel file
#
if args.file2:

   waterlevel2_file = args.file2
   if not os.path.isfile(waterlevel2_file):
      message  = "Name of second waterlevel file listing groundwater measurements %s does not exist" % waterlevel2_file
      errorMessage(message)

if args.log:

   screen_logger.setLevel(logging.INFO)

   logging_file = "compareWaterlevelLogFile.txt"
   if os.path.isfile(logging_file):
      os.remove(logging_file)

   formatter    = logging.Formatter('%(message)s')
   handler      = logging.FileHandler(logging_file)
   handler.setFormatter(formatter)
   file_logger  = logging.getLogger('file_logger')
   file_logger.setLevel(logging.INFO)
   file_logger.addHandler(handler)
   file_logger.propagate = False

# Set
#
siteInfoD    = {}
   
# Read collection file and set column names
#
mySiteFields = [
                "site_id",
                "agency_cd",
                "site_no",
                "coop_site_no",
                "cdwr_id",
                "state_well_nmbr",
                "station_nm",
                "dec_lat_va",
                "dec_long_va",
                "alt_va",
                "alt_acy_va",
                "alt_datum_cd"
               ]

# Parse entire file
#
with open(collection_file,'r') as f:
    linesL = f.read().splitlines()

# Check for empty file
#
if len(linesL) < 1:
   message = "Empty collection file %s" % collection_file
   errorMessage(message)

# Process file
#
siteInfoD = processCollectionSites("site_id", mySiteFields, linesL)


# Read waterlevel file and set column names
#
myGwFields  =  [
                "site_id",
                "site_no",
                "agency_cd",
                "coop_site_no",
                "cdwr_id",
                "lev_va",
                "lev_acy_cd",
                "lev_dtm",
                "lev_dt",
                "lev_tm",
                "lev_tz_cd",
                "lev_dt_acy_cd",
                "lev_str_dt",
                "lev_status_cd",
                "lev_meth_cd",
                "lev_agency_cd",
                "lev_src_cd",
                "lev_web_cd"
               ]
myGwFormats =  [
                "%20s", # site_id
                "%10s", # agency_cd
                "%20s", # site_no
                "%15s", # coop_site_no
                "%20s", # cdwr_id
                "%10s", # lev_va
                "%10s", # lev_acy_cd
                "%20s", # lev_dtm
                "%10s", # lev_dt
                "%10s", # lev_tm
                "%10s", # lev_tz_cd
                "%16s", # lev_dt_acy_cd
                "%20s", # lev_str_dt
                "%16s", # lev_status_cd
                "%16s", # lev_meth_cd
                "%16s", # lev_agency_cd
                "%10s", # lev_src_cd
                "%10s"  # lev_web_cd
               ]

# Parse first waterlevel file
#
with open(waterlevel1_file,'r') as f:
    linesL = f.read().splitlines()

# Check for empty file
#
if len(linesL) < 1:
   message = "Empty waterlevel file %s" % waterlevel1_file
   errorMessage(message)

# Process file
#
waterlevelFile1D = processWls(waterlevel1_file, "site_id", myGwFields, linesL)

# Parse second waterlevel file
#
with open(waterlevel2_file,'r') as f:
    linesL = f.read().splitlines()

# Check for empty file
#
if len(linesL) < 1:
   message = "Empty waterlevel file %s" % waterlevel2_file
   errorMessage(message)

# Process file
#
waterlevelFile2D = processWls(waterlevel2_file, "site_id", myGwFields, linesL)


# Prepare output
# -------------------------------------------------
#

# Site information
#
mySiteFields = [
                "site_id",
                "agency_cd",
                "site_no",
                "coop_site_no",
                "cdwr_id",
                "state_well_nmbr",
                "station_nm"
               ]
mySiteFormat = [
                "%20s", # site_id
                "%10s", # agency_cd
                "%20s", # site_no
                "%15s", # coop_site_no
                "%20s", # cdwr_id
                "%20s", # state_well_nmbr
                "%30s", # station_nm
               ]

localDate    = datetime.datetime.now().strftime("%B %d, %Y")

# Prepare column heading line
#
ncol        = 200
recordsL    = []
recordL     = []
recordsL.append("\n\n")

message = "Groundwater Information Comparison"
fmt     = "%" + str(int(ncol/2-len(message)/2)) + "s%s"
recordsL.append(fmt % (' ', message))
recordsL.append("=" * ncol)
message = "Recorded on %s" % localDate
fmt     = "%" + str(int(ncol/2-len(message)/2)) + "s%s"
recordsL.append(fmt % (' ', message))
recordsL.append("-" * ncol)

headLine = []
for i in range(len(mySiteFields)):
   myColumn = mySiteFormat[i] % mySiteFields[i]
   headLine.append(myColumn)

headLine.append('%20s' % 'Measurement Date')
headLine.append('%s' % 'Column')
                       
recordsL.append("   ".join(headLine))

recordsL.append("-" * ncol)


# Loop through site information
#
missingL  = []

siteCount = 0

# Build list of dates from both waterlevel files
#
Site1L = set(waterlevelFile1D.keys())
Site2L = set(waterlevelFile2D.keys())
SitesL = list(Site1L.intersection(Site2L))

# Loop through sites in both waterlevel files
#
for site_id in sorted(SitesL):

   siteCount  += 1
   waterlevelD = {}

   # Build list of dates from both waterlevel files
   #
   Date1L = set(waterlevelFile1D[site_id].keys())
   Date2L = set(waterlevelFile2D[site_id].keys())
   DatesL = list(Date1L.intersection(Date2L))
   
   # Build list of non-common dates from both waterlevel files
   #
   Only1L = list(Date1L.difference(Date2L))
   Only2L = list(Date2L.difference(Date1L))
   
   # Loop data in both waterlevel files
   #
   for myDate in sorted(DatesL):

      itemsL = []
   
      # Check matching date record in both waterevel files
      #
      for myColumn in myGwFields:

          if myColumn in mySiteFields:
             continue

          if myColumn == 'lev_dtm':
             continue

          elif myColumn == 'lev_acy_cd':
             continue

          if myColumn == 'lev_va':
             if len(waterlevelFile1D[site_id][myDate][myColumn]) > 0:
                column1 = str(float(waterlevelFile1D[site_id][myDate][myColumn]))
             if len(waterlevelFile2D[site_id][myDate][myColumn]) > 0:
                column2 = str(float(waterlevelFile2D[site_id][myDate][myColumn]))

          else:
             column1 = str(waterlevelFile1D[site_id][myDate][myColumn])
             column2 = str(waterlevelFile2D[site_id][myDate][myColumn])

          # Compare column values
          #
          if column1.strip() != column2.strip():
             itemsL.append(myColumn)
             if site_id == '413936121490601':
                screen_logger.info('Site %s: myColumn %s column1 |%s| column2 |%s|' % (site_id, myColumn, column1, column2))
   
      # Record issue
      #
      if len(itemsL) > 0:
         waterlevelD[myDate] = itemsL
    
   # Loop data only in first waterlevel file, missing second waterlevel file
   #
   for myDate in sorted(Only1L):
      waterlevelD[myDate] = 'Missing in second waterlevel file'
    
   # Loop data only in second waterlevel file, missing first waterlevel file
   #
   for myDate in sorted(Only2L):
      waterlevelD[myDate] = 'Missing in first waterlevel file'   
   
   # Loop through issues
   #
   if len(waterlevelD) > 0:
      
      # Record site information
      #
      recordL = []
      for i in range(len(mySiteFields)):
         recordL.append(mySiteFormat[i] % siteInfoD[site_id][mySiteFields[i]])

      recordsL.append("   ".join(recordL))
      
      for myDate in sorted(waterlevelD.keys()):
         
         # Record issue
         #
         if isinstance(waterlevelD[myDate], str):
            recordsL.append("\t\t%-20s  %s" % (myDate[:10], waterlevelD[myDate]))
         
         # Record issue
         #
         else:
            recordL = []
            myItems = waterlevelD[myDate]
   
            for i in range(len(myGwFields)):
               if myGwFields[i] in myItems:
                  recordL.append(myGwFormats[i] % myGwFields[i])
                  
            recordsL.append("\t\t%-20s  %s" % (myDate[:10], recordL))
               
            if myDate in waterlevelFile1D[site_id]:
               recordL = []
               for i in range(len(myGwFields)):
                  if myGwFields[i] in myItems:
                     recordL.append(myGwFormats[i] % waterlevelFile1D[site_id][myDate][myGwFields[i]])
      
               recordsL.append("\t\t     %-15s  %s" % ('File 1 ->', recordL))
      
            if myDate in waterlevelFile2D[site_id]:
               recordL = []
               for i in range(len(myGwFields)):
                  if myGwFields[i] in myItems:
                     recordL.append(myGwFormats[i] % waterlevelFile2D[site_id][myDate][myGwFields[i]])
      
               recordsL.append("\t\t     %-15s  %s" % ('File 2 ->', recordL))


# Write file [Sites with waterlevels]
# -------------------------------------------------

localDate = datetime.datetime.now().strftime("%B %d, %Y")

# Print header information
#
ncol    = 200
outputL = []
outputL.append("## Upper Klamath Basin Compare Waterlevel Information")
outputL.append("##" * ncol)
outputL.append("## Recorded on %30s" % localDate)
outputL.append("##%s" % ("-" * ncol))
outputL.append('\n')
outputL.append("%s" % '\n'.join(outputL))
outputL.append('\n\n')
file_logger.info("%s" % '\n'.join(recordsL))
file_logger.info('\n')

# Write file [Sites with no waterlevels]
# -------------------------------------------------
output_file = "wellmissing.txt"
if os.path.isfile(output_file):
   os.remove(output_file)

# Open file
#
fh = open(output_file, 'w')
if fh is None:
   message = "Can not open output file %s" % output_file
   errorMessage(message)

fh.write("%s" % '\n'.join(missingL))

fh.close()


sys.exit()

