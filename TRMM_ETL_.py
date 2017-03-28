#-------------------------------------------------------------------------------
# Name:        TRMM_ETL.py
# Purpose:     SERVIR Implementation of TRMM ETL Scripts for various ArcGIS products and services.
#
#
# Author:      Kris Stanton
# Last Modified By: Githika Tondapu
# Created:     2014
# Copyright:   (c) kstanto1 2014
# Licence:     <your licence>
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------

# Notes
# Files that are extracted for TRMM are in an expected filename format
# 'TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif.gz' as an example

import arcpy
from arcpy import env
import datetime
import time
import os
import urllib
import urllib2
import sys
import zipfile
import gzip
import shutil
import json
import ftplib
import re 
import pickle
# SD's files: "arcpy_utils.py" and "etl_utls"
from arcpy_utils import FileGeoDatabase, RasterCatalog, AGServiceManager
from etl_utils import FTPDownloadManager

# SD's file for creating static maps.
from arcpy_trmm_custom_raster import TRMMCustomRasterRequest, TRMMCustomRasterCreator
from copy import deepcopy

# External Libs in support of ETL processes
import boto     # For Amazon S3 Interfacing

# ETL Support Items (Used in ALL ETLs)
import ks_ConfigLoader      # Handles loading the xml config file
import ks_AdpatedLogger     # Handles logging items in a standardized way

#--------------------------------------------------------------------------
# Global Variables
#   Place Globally scoped variables here.
#--------------------------------------------------------------------------

# The only hard coded item in this whole script.  This is the location of the config file of which the contents are then used in script execution.
#g_PathToConfigFile = r"C:\kris\!!Work\ETL_TRMM\config\config_TRMM.xml"
g_PathToConfigFile = r"D:\SERVIR\Scripts\TRMM\config_TRMM.xml"

# Load the Config XML File into a settings dictionary
g_ConfigSettings = ks_ConfigLoader.ks_ConfigLoader(g_PathToConfigFile)

# Detailed Logging Setting, Default to False
g_DetailedLogging_Setting = False

#--------------------------------------------------------------------------
# Settings and Logger
#   Code that initializes and supports getting settings
#   and interfacing with the logger funcitonality
#--------------------------------------------------------------------------

# Loads the Settings object.
def get_Settings_Obj():
    Current_Config_Object = g_ConfigSettings.xmldict['ConfigObjectCollection']['ConfigObject']
    return Current_Config_Object

# Needed to prevent errors (while the 'printMsg' function is global...)
settingsObj = get_Settings_Obj()
# Logger Settings Vars
theLoggerOutputBasePath = settingsObj['Logger_Output_Location'] # Folder where logger output is stored.
theLoggerPrefixVar = settingsObj['Logger_Prefix_Variable'] # String that gets prepended to the name of the log file.
theLoggerNumOfDaysToStore = settingsObj['Logger_Num_Of_Days_To_Keep_Log'] # Number of days to keep log
# KS Mod, 2014-01   Adding a Script Logger 3        START
g_theLogger = ks_AdpatedLogger.ETLDebugLogger(theLoggerOutputBasePath, theLoggerPrefixVar+"_log", {

        "debug_log_archive_days":theLoggerNumOfDaysToStore
    })


# Add to the log
def addToLog(theMsg, detailedLoggingItem = False):

    global g_theLogger, g_DetailedLogging_Setting
    if detailedLoggingItem == True:
        if g_DetailedLogging_Setting == True:
            # This configuration means we should record detailed log items.. so do nothing
            pass
        else:
            # This config means we should NOT record detailed log items but one was passed in, so using 'return' to skip logging
            return

    # These lines wrap each log entry onto a new line prefixed by the date/time of code execution

    currText = ""
    currText += theMsg


    g_theLogger.updateDebugLog(currText)

# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % (seconds)
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)

# Get a new time object
def get_NewStart_Time():
    timeStart = time.time()
    return timeStart

# Get the amount of time elapsed from the input time.
def get_Elapsed_Time_As_String(timeInput):
    return timeElapsed(timeInput)


# Parse "0" or "1" from settings into a bool.
def get_BoolSetting(theSetting):
    try:
        if theSetting == "1":
            return True
        else:
            return False
    except:
        addToLog("get_BoolSetting: SCRIPT ERROR!! ERROR PARSING BOOL SETTING FOR (theSetting), " + str(theSetting) + ", Returning False")
        return False


# Release Candidate Function for implementation
# Force item to be in a list
def convert_Obj_To_List(item_Object):
    retList = list()

    # Quick test to see if the item is already a list
    testList = []
    isAlreadyList = False
    try:
        testList + item_Object
        isAlreadyList = True
    except:
        isAlreadyList = False

    # if the item is already a list, return it, if not, add it to an empty one.
    if isAlreadyList == True:
        return item_Object
    else:
        retList.append(item_Object)
        return retList


# Makes a directory on the filesystem if it does not already exist.
# Then checks to see if the folder exists.
# Returns True if the folder exists, returns False if it does not
def make_And_Validate_Folder(thePath):
    try:
        # Create a location for the file if it does not exist..
        if not os.path.exists(thePath):
            os.makedirs(thePath)
        # Return the status
        return os.path.exists(thePath)
    except:
        e = sys.exc_info()[0]
        addToLog("make_And_Validate_Folder: ERROR, Could not create folder at location: " + str(thePath) + " , ERROR MESSAGE: "+ str(e))
        return False


# returns todays date minus the interval ("90 days") for example
def Unsorted_GetOldestDate(intervalString):
    try:
        intervalValue = int(intervalString.split(" ")[0])
        intervalType = intervalString.split(" ")[1]

        deltaArgs = {intervalType:intervalValue}

        # Get the oldest date before now based on the interval and date format
        oldestDate = datetime.datetime.utcnow() - datetime.timedelta(**deltaArgs)
    except:
        e = sys.exc_info()[0]
        print("    Error getting oldest date: System Error message: "+ str(e))
        return None

    return oldestDate


# Remove old raster(s) from the mosaic dataset(s) and remove the files from
#   the file system if they get removed from the mosaic dataset
#   Return the number of rasters removed
def Unsorted_removeRastersMosaicDataset(varList,mdWS,oldDate,qryDateFmt):
    numRemoved = 0
    for varDict in varList:
        mosaicDSName = varDict["mosaic_name"]
        dateField = varDict["primary_date_field"]
        mosaicDS = os.path.join(mdWS, mosaicDSName)

        if not dateField:
            addToLog("Unsorted_removeRastersMosaicDataset: No primary date field defined for "+mosaicDSName+".  No rasters removed")
            pass
        else:
            dstr = oldDate.strftime(qryDateFmt)
            query = dateField + " < date '" + dstr + "'"
            addToLog("Unsorted_removeRastersMosaicDataset: query "+str(query), True)

            try:
                # Remove the rasters from the mosaic dataset based on the query
                startCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))
                arcpy.RemoveRastersFromMosaicDataset_management(mosaicDS, str(query), "NO_BOUNDARY", "NO_MARK_OVERVIEW_ITEMS", \
                                                                "NO_DELETE_OVERVIEW_IMAGES", "NO_DELETE_ITEM_CACHE", \
                                                                "REMOVE_MOSAICDATASET_ITEMS", "NO_CELL_SIZES")
                endCount = int(arcpy.GetCount_management(mosaicDS).getOutput(0))

                addToLog("Unsorted_removeRastersMosaicDataset: Removed "+str(startCount-endCount)+" rasters ("+str(query)+") from "+str(mosaicDSName))
                numRemoved = numRemoved + (startCount-endCount)

            # Handle errors for removing rasters
            except:

                addToLog("Unsorted_removeRastersMosaicDataset: Error removing rasters from "+mosaicDSName+", ArcPy message"+str(arcpy.GetMessages()))
                pass

    return numRemoved


# Cleans up old files from the output raster location (file system)
def Unsorted_dataCleanup(rasterOutputLocation,oldDate, regExp_Pattern, rastDateFormat): #,dateFmt):
    numDeleted = 0

    arcpy.env.workspace = rasterOutputLocation
    dateFmt = "%Y%m%d%H"
    oldDateStr = oldDate.strftime(dateFmt)
    oldDateInt = int(oldDateStr)
    addToLog("dataCleanup: Deleting rasters older than, "+str(oldDateInt))

    try:
        for raster in arcpy.ListRasters("*", "All"):
            rasterDatesFoundList = re.findall(regExp_Pattern,str(raster))
            rastDateStr = rasterDatesFoundList[0]
			
            # Convert to a date format that 'int()' can understand.
            tempDateTime = datetime.datetime.strptime(rastDateStr, rastDateFormat)
            tempDateTimeStr = tempDateTime.strftime(dateFmt)
            rastDateInt = int(tempDateTimeStr)

            # KS Refactor..  if a delete operation fails, the code keeps on going and tries the next one....
            try:
                if(oldDateInt > rastDateInt):
                    arcpy.Delete_management(raster)
                    addToLog ("dataCleanup: Deleted "+raster, True)
                numDeleted = numDeleted + 1
            except:
                addToLog("dataCleanup: Error Deleting "+raster+" ArcPy Message: "+str(arcpy.GetMessages()))


    # Handle errors for deleting old raster files
    except:
        addToLog("dataCleanup: Error cleaning up old raster files from "+rasterOutputLocation+" ArcPy Message: "+str(arcpy.GetMessages()))

    return numDeleted

#--------------------------------------------------------------------------
# Pre ETL
#   Processes that must be performed prior to the ETL process
#   This may include things like gathering a list of existing items and
#   comparing it with existing lists to deterimine what new items should
#   be downloaded.
#--------------------------------------------------------------------------

# Converts XML read var dictionary settings into a standard "VarDictionary" object
#  Sometimes "ListItem" and "service_dict_list" only contain one element.  When that happens, their types need to be converted to lists.
#  This method handles that conversion.
def PreETL_Support_Get_Standard_VarDictionary_From_RawVarSettings(RawVarSettings):
    # Force the entire item to be a list
    varSettings_1 = convert_Obj_To_List(RawVarSettings)

    # The root level entry, called, "ListItem" also needs to be forced into a list
    listItem_List = convert_Obj_To_List(varSettings_1[0]['ListItem'])

    # For each list item, we need to make sure that the child element, 'service_dict_list' is ALSO a list.
    rebuilt_ListItem_List = list()
    for currListItem in listItem_List:
        currListItem['service_dict_list'] = convert_Obj_To_List(currListItem['service_dict_list'])
        rebuilt_ListItem_List.append(currListItem)

    # Now rebuild the Return object
    retVarDict = rebuilt_ListItem_List

    # Return the result
    return retVarDict

# Validate Config, Create Workspaces
def PreETL_Support_CreateWorkspaceFolders(theScratchWorkspace_BasePath):
    # Assemble the input folder paths to create.
    workSpacePath_PreETL = theScratchWorkspace_BasePath + "\\PreETL"
    workSpacePath_Extract = theScratchWorkspace_BasePath + "\\Extract"
    workSpacePath_Transform = theScratchWorkspace_BasePath + "\\Transform"
    workSpacePath_Load = theScratchWorkspace_BasePath + "\\Load"
    workSpacePath_PostETL = theScratchWorkspace_BasePath + "\\PostETL"

    # Create the folders and set the flag if any fail.
    foldersExist = True
    checkList = list()
    checkList.append(make_And_Validate_Folder(workSpacePath_PreETL))
    checkList.append(make_And_Validate_Folder(workSpacePath_Extract))
    checkList.append(make_And_Validate_Folder(workSpacePath_Transform))
    checkList.append(make_And_Validate_Folder(workSpacePath_Load))
    checkList.append(make_And_Validate_Folder(workSpacePath_PostETL))
    if False in checkList:
        foldersExist = False

    # package up the return object
    retObj = {
        "PreETL":workSpacePath_PreETL,
        "Extract":workSpacePath_Extract,
        "Transform":workSpacePath_Transform,
        "Load":workSpacePath_Load,
        "PostETL":workSpacePath_PostETL,
        "FoldersExist": foldersExist
    }

    return retObj

# Returns True if the workspace path and type are valid, Returns False if not valid or on error.
def PreETL_Support_Validate_Dataset_Workspace(theWorkspacePath):
    try:
        if not arcpy.Exists(theWorkspacePath):
            addToLog("PreETL_Support_Validate_Dataset_Workspace: Error: Workspace path, "+str(theWorkspacePath)+", does not exist")
            return False
        else:
            addToLog("PreETL_Support_Validate_Dataset_Workspace: about to arcpy.Describe the workspace path, "+str(theWorkspacePath), True)
            descWS = arcpy.Describe(theWorkspacePath)
            if not descWS.dataType == "Workspace":
                addToLog("PreETL_Support_Validate_Dataset_Workspace: Error: The Workspace must be of datatype 'Workspace'.  The current datatype is: "+str(descWS.dataType))
                return False
            else:
                return True
    except:
        e = sys.exc_info()[0]
        addToLog("PreETL_Support_Validate_Dataset_Workspace: ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
        return False
    return False

# Returns True if the output raster directory exists or gets created.  Returns False on error
def PreETL_Support_Create_RasterOutput_Location(theRasterOutputPath):
    return make_And_Validate_Folder(theRasterOutputPath)


# This function would be called by the main controller and would either just execute some simple process, or call on the support method(s) immediately above to execute a slightly more complex process.
def PreETL_Controller_Method(ETL_TransportObject):

    # Any other PreETL procedures could go here...

    # Make the Variable Dictionary Object
    addToLog("PreETL_Controller_Method: Validating Variable_Dictionary_List", True)
    Variable_Dictionary_List = PreETL_Support_Get_Standard_VarDictionary_From_RawVarSettings(ETL_TransportObject['SettingsObj']['VariableDictionaryList'])

    # Validate Config - Create Workspace folders
    addToLog("PreETL_Controller_Method: Validating Scratch_WorkSpace_Locations", True)
    Scratch_WorkSpace_Locations = PreETL_Support_CreateWorkspaceFolders(ETL_TransportObject['SettingsObj']['ScratchFolder'])

    # Validate Config - Make sure the data set work space exists (Path to GeoDB or SDE connection)
    addToLog("PreETL_Controller_Method: Joining Folders to create GeoDB_Dataset_Workspace", True)
    GeoDB_Dataset_Workspace = os.path.join(ETL_TransportObject['SettingsObj']['GeoDB_Location'], ETL_TransportObject['SettingsObj']['GeoDB_FileName'])
    addToLog("PreETL_Controller_Method: Validating GeoDB_Dataset_Workspace", True)
    is_Dataset_Workspace_Valid = PreETL_Support_Validate_Dataset_Workspace(GeoDB_Dataset_Workspace)

    # Validate Config - Make sure the output Raster Directory exists.
    RasterOutput_Location = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    is_RasterOutLocation_Valid = PreETL_Support_Create_RasterOutput_Location(RasterOutput_Location)

    # Any other PreETL procedures could also go here...


    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    # Validate - Checking if scratch workspace folders were created
    if Scratch_WorkSpace_Locations['FoldersExist'] == False:
        IsError = True
        ErrorMessage += "ERROR: One of the scratch workspace folders was unable to be created.  | "

    # Validate - Checking if workspace is valid
    if is_Dataset_Workspace_Valid == False:
        IsError = True
        ErrorMessage += "ERROR: The arc workspace either does not exist or is of an invalid type.  | "

    # Validate - Make sure raster output path exists or was created
    if is_RasterOutLocation_Valid == False:
        IsError = True
        ErrorMessage += "ERROR: The raster output location, " + str(RasterOutput_Location) + ", does not exist or was unable to be created."


    # Package up items from the PreETL Step
    returnObj = {
        'Variable_Dictionary_List': Variable_Dictionary_List,
        'Scratch_WorkSpace_Locations': Scratch_WorkSpace_Locations,
        'GeoDB_Dataset_Workspace':GeoDB_Dataset_Workspace,
        'RasterOutput_Location':RasterOutput_Location,

        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj



#--------------------------------------------------------------------------
# Extract
#   The function(s) that perform the Extraction step.
#   Typically, this involves reading the input datastructure,
#   Connecting to an ftp, web or s3 server, downloading files,
#   and finally extracting them to a temp location.
#   The last step of this process is usually to write data to the return object
#   with information about the extraction step so that the Transform
#   step can use that as an input.
#--------------------------------------------------------------------------


#def Extract_Support_GetStartDate(mosaicName, primaryDateField, mosaicDS):
def Extract_Support_GetStartDate(primaryDateField, mosaicDS):
    startDate = None
    try:
        sortedDates = sorted([row[0] for row in arcpy.da.SearchCursor(mosaicDS,primaryDateField)])
    except:
        e = sys.exc_info()[0]
    try:
        maxDate = sortedDates[-1]
        if (not startDate) or (maxDate < startDate):
            startDate = maxDate

    except: 
        startDate = datetime.datetime.now() + datetime.timedelta(-90)# datetime.timedelta(-30) #maxDate

    if startDate == None:
        startDate = datetime.datetime.now() + datetime.timedelta(-90)
    return startDate

def Extract_Support_GetEndDate():
    return datetime.datetime.utcnow()

# Simillar to the function Extract_Support_Get_PyDateTime_From_String, but returns only the string component.
def Extract_Support_Get_DateString_From_String(theString, regExp_Pattern):
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern,theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            # If needed, this is where to insert a log entry or other notification that no date was found.
            return None
        else:
            return reItemsList[0]
    except:
        return None

# Search a string (or filename) for a date by using the regular expression pattern string passed in,
# Then use the date format string to convert the regular expression search output into a datetime.
# Return None if any step fails.
def Extract_Support_Get_PyDateTime_From_String(theString, regExp_Pattern, date_Format):
    try:
        # Search the string for the datetime format
        reItemsList = re.findall(regExp_Pattern,theString)
        if len(reItemsList) == 0:
            # No items found using the Regular expression search
            # If needed, this is where to insert a log entry or other notification that no date was found.
            return None
        else:
            retDateTime = datetime.datetime.strptime(reItemsList[0], date_Format)
            return retDateTime
    except:
        return None



# Support Method which returns a list of files that fall within the passed in date range.
def Extract_Support_GetList_Within_DateRange(the_ListOf_AllFiles, the_FileExtn, the_Start_DateTime, the_End_DateTime, regExp_Pattern, date_Format):
    retList = []

    # Sort of a validation step.. I think.. looks like we just split file names.. not sure why the original author wrote this code but it works
    # FUTURE: Optimize, check to see if we actually need this step..
    #   (I think this is in case there is a mix of files in the folder and we only want a subset of them.... or maybe folder names get placed in the list sometimes which have no extension.. these all sound like good reasons to keep this here)
    list_Of_FileNames = []
    if the_FileExtn:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_ListOf_AllFiles if f.endswith(the_FileExtn)]
    else:
        list_Of_FileNames = [f.split(" ")[-1] for f in the_ListOf_AllFiles]

    # Now iterate through the list and only add the ones that match the critera
    for currFileName in the_ListOf_AllFiles:
        currFileNameDateTime = Extract_Support_Get_PyDateTime_From_String(currFileName, regExp_Pattern, date_Format)
        try:
            if ((currFileNameDateTime > the_Start_DateTime) and (currFileNameDateTime <= the_End_DateTime)):
                retList.append(currFileName)
        except:
            # String probably was "None" type, try the next one!
            pass

    return retList



# Gets and returns a list of files contained in the bucket and path.
#   Access Keys are required and are used for making a connection object.
def Extract_Support_s3_GetFileListForPath(s3_AccessKey,s3_SecretKey,s3_BucketName, s3_PathToFiles, s3_Is_Use_Local_IAMRole):

    # Refactor for IAM Role
    # s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey)
    s3_Connection = None
    if s3_Is_Use_Local_IAMRole == True:
        try:
            s3_Connection = boto.connect_s3(is_secure=False)
        except:
            s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey,is_secure=False)
    else:
        s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey,is_secure=False)

    s3_Bucket = s3_Connection.get_bucket(s3_BucketName,True,None)
    s3_ItemsList = list(s3_Bucket.list(s3_PathToFiles))
    retList = []
    for current_s3_Item in s3_ItemsList:
        retList.append(current_s3_Item.key)
    return retList


# Takes in a key and converts it to a URL.
def Extract_Support_s3_Make_URL_From_Key(s3_BucketRootPath, current_s3_Key):
    # Sample URL    3 (yes, 2 slashes, does not work with only 1)
    # https://bucket.servirglobal.net.s3.amazonaws.com//regions/africa/data/eodata/crest/TIFQPF2014021812.zip
    retString = str(s3_BucketRootPath) + str(current_s3_Key)
    return retString

# Get the file name portion of an S3 Key Path
def Extract_Support_Get_FileNameOnly_From_S3_KeyPath(theS3KeyPath):
    retStr = theS3KeyPath.split('/')[-1]
    return retStr


# Try and Extract a 'gz' file, if success, return True, if fail, return False
# Example inputs
# inFilePath = r"d:\fullpath\TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif.gz"
# outFilePath = r"d:\fullpath\TRMM-3B42RT-V7-Rain_2014-05-03T03Z.tif",
# inFileExt = "tif.gz"
def Extract_Support_Decompress_GZip_File(inFilePath, outFilePath, inFileExt):
    # make sure the format is correct.
    if "GZ" in inFileExt.upper():
        try:
            inF = gzip.open(inFilePath, 'rb')
            outF = open(outFilePath, 'wb')
            outF.write( inF.read() )
            inF.close()
            outF.close()
            addToLog("Extract_Support_Decompress_GZip_File: Extracted file from, " + str(inFilePath) + " to " + str(outFilePath), True)
            return True
        except:
            e = sys.exc_info()[0]
            addToLog("Extract_Support_Decompress_GZip_File: ERROR extracting the gz File " + str(inFilePath) + " Error Message: " + str(e))
            return False
    else:
        # File extension is incorrect.
        addToLog("Extract_Support_Decompress_GZip_File: ERROR, File " + str(inFilePath) + " has an unexpected file extension.")
        return False




# Get the file names, filter the list, download the files, extract them, wrap them into a list, return a results object
# Goes into the S3, downloads files, extracts them, returns list of items
def Extract_Do_Extract_S3(the_FileExtension, s3BucketRootPath, s3AccessKey, s3SecretKey, s3BucketName, s3PathTo_Files, s3_Is_Use_Local_IAM_Role, regEx_String, dateFormat_String, startDateTime_str, endDateTime_str, theExtractWorkspace):

    ExtractList = []
    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0
    debugFileDownloadLimiter = 10000 #10000        # For debugging, set this to a low number

    # Get the Start / End datetimes
    startDateTime = datetime.datetime.strptime(startDateTime_str, dateFormat_String)
    endDateTime = datetime.datetime.strptime(endDateTime_str, dateFormat_String)

     # get a list of ALL files from the bucket and path combo.
    theListOf_BucketPath_FileNames = Extract_Support_s3_GetFileListForPath(s3AccessKey,s3SecretKey,s3BucketName,s3PathTo_Files, s3_Is_Use_Local_IAM_Role)

    # get a list of all the files within the start and end date
    filePaths_WithinRange = Extract_Support_GetList_Within_DateRange(theListOf_BucketPath_FileNames, the_FileExtension, startDateTime, endDateTime, regEx_String, dateFormat_String)

    numFound = len(filePaths_WithinRange)
    if numFound == 0:
        if startDateTime_str == endDateTime_str:
            addToLog("Extract_Do_Extract_S3: ERROR: No files found for the date string "+startDateTime_str)
        else:
            addToLog("Extract_Do_Extract_S3: ERROR: No files found between "+startDateTime_str+" and "+endDateTime_str)
    else:

        # Iterate through each key file path and and perform the extraction.
        for s3_Key_file_Path_to_download in filePaths_WithinRange:
            if counter_FilesDownloaded < debugFileDownloadLimiter:
                file_to_download = Extract_Support_Get_FileNameOnly_From_S3_KeyPath(s3_Key_file_Path_to_download)

                # This is the location where the file will be downloaded.
                downloadedFile = os.path.join(theExtractWorkspace,file_to_download)   # Actual Code

                # Get final download path (URL to where the file is located on the internets.
                currentURL_ToDownload = Extract_Support_s3_Make_URL_From_Key(s3BucketRootPath, s3_Key_file_Path_to_download)

                # Do the actual download.
                try:
                    theDLodaedURL = urllib.urlopen(currentURL_ToDownload)
                    open(downloadedFile,"wb").write(theDLodaedURL.read())
                    theDLodaedURL.close()
                    addToLog("Extract_Do_Extract_S3: Downloaded file from: " + str(currentURL_ToDownload), True)
                    addToLog("Extract_Do_Extract_S3: Downloaded file to: " + str(downloadedFile), True)
                    counter_FilesDownloaded += 1

                    # Extract the zipped file, (NOTE, THIS IS FOR GZIP FILES, files with extension of .gz)
                    # Also, the expected end of the file name is, ".tif.gz"
                    # Last Note, this function only unzips a single file
                    theOutFile = downloadedFile[:-3] # Should be the whole file path except the '.gz' part.
                    ungzipResult = Extract_Support_Decompress_GZip_File(downloadedFile, theOutFile, the_FileExtension)
                    if ungzipResult == True:
                        # Extraction worked, create the return item
                        extractedFileList = []
                        extractedFileList.append(theOutFile)
                        currentDateString = Extract_Support_Get_DateString_From_String(theOutFile, regEx_String)
                        current_Extracted_Obj = {
                            'DateString' : currentDateString,
                            'Downloaded_FilePath' : downloadedFile,
                            'ExtractedFilesList' : convert_Obj_To_List(extractedFileList),
                            'downloadURL' : currentURL_ToDownload
                        }
                        ExtractList.append(current_Extracted_Obj)
                        counter_FilesExtracted += 1
                    else:
                        # Extraction failed, Add this to the log..
                        addToLog("Extract_Do_Extract_S3: ERROR, There was a problem decompressing the file, " + str(downloadedFile))

                except:
                    e = sys.exc_info()[0]
                    addToLog("Extract_Do_Extract_S3: ERROR: Could not download file: " + str(theDLodaedURL) + ", Error Message: " + str(e))
    ret_ExtractObj = {
        'StartDateTime':startDateTime,
        'EndDateTime': endDateTime,
        'ExtractList':ExtractList
    }
    return ret_ExtractObj



# FTP Extract


# Update, Params returned in list,
#  Each object has these,
#   FTPFolderPath
#   BaseRasterName
#   FTP_PathTo_TIF
#   FTP_PathTo_TFW
# root_FTP_Path = "ftp://trmmopen.gsfc.nasa.gov/pub/gis"
# Returns objects which are, { partialRasterName: "3B42RT.2014052203.7.", basePath: "ftp://someftppath",  baseRasterName: "3B42RT.2014052203.7.03hr" , pathTo_TIF: "ftp://someftppath/3B42RT.2014052203.7.03hr.tif", pathTo_TFW: "someftppath/3B42RT.2014052203.7.03hr.tfw"}
# No 'basePath', instead, using 'FTPFolderPath'  (Path to the folder containing the current expected tif file.
# Partial Raster Name part can be used to construct a download for the 1day, or 3day or 7day files by appending, ".1day.tif" and ".1day.tfw" for example.  (Ofcourse the base path needs to be prepended to this to construct a full download link.)
# Each object represents a single raster with 2 files to download.
def Extract_Support_Get_Expected_FTP_Paths_From_DateRange(start_DateTime, end_DateTime, root_FTP_Path, the_FTP_SubFolderPath):
    retList = []

    #counter = 0

    the_DateFormatString = "%Y%m%d%H"
    the_FileNamePart1 = "3B42RT."        # TRMM Product ID?
    the_FileNameEnd_3Hr_Base = ".7.03hr" # Version and time frame
    the_FileNameEnd_Tif_Ext = ".tif"     # Tif file
    the_FileNameEnd_Tfw_Ext = ".tfw"     # World File

    # Unused, for reference     # a tif and tfw also exist for each of these..
    the_FileNameEnd_1day_Base = ".7.1day" # Version and time composit product
    the_FileNameEnd_3day_Base = ".7.3day" # Version and time composit product
    the_FileNameEnd_7day_Base = ".7.7day" # Version and time composit product


    currentDateTime = start_DateTime
    while currentDateTime < end_DateTime:
        # Do processing
        # Build all the object props based on currentDateTime and filenames etc.. BUILD the folder paths
        currentDateString = currentDateTime.strftime(the_DateFormatString)
        currentYearString = currentDateTime.strftime("%Y")
        currentMonthString = currentDateTime.strftime("%m")

        currentRasterBaseName = the_FileNamePart1 + currentDateString + the_FileNameEnd_3Hr_Base
        currentFTP_Subfolder = the_FTP_SubFolderPath + "/" + currentYearString + currentMonthString
        currentFTPFolder = root_FTP_Path + "/" + currentYearString + currentMonthString

        current_3Hr_Tif_Filename = currentRasterBaseName + the_FileNameEnd_Tif_Ext
        current_3Hr_Twf_Filename = currentRasterBaseName + the_FileNameEnd_Tfw_Ext

        currentPathToTif = currentFTPFolder + "/" + current_3Hr_Tif_Filename
        currentPathToTwf = currentFTPFolder + "/" + current_3Hr_Twf_Filename

        # Load object
        # Create an object loaded with all the params listed above
        currentObj = {
            "FTPFolderPath" : currentFTPFolder,                 # "ftp://someftppath/yyyymm", # No / at the end
            "FTPSubFolderPath" : currentFTP_Subfolder,              # "someftppath/yyyymm", # No / at the end
            "BaseRasterName" : currentRasterBaseName,           # "3B42RT.2014052203.7.03hr" ,
            "FTP_PathTo_TIF" : currentPathToTif,                # "ftp://someftppath/3B42RT.2014052203.7.03hr.tif",
            "FTP_PathTo_TFW" : currentPathToTwf,                # "someftppath/3B42RT.2014052203.7.03hr.tfw"
            "TIF_3Hr_FileName" : current_3Hr_Tif_Filename,      # "3B42RT.2014052203.7.03hr.tif",
            "TWF_3Hr_FileName" : current_3Hr_Twf_Filename,       # "3B42RT.2014052203.7.03hr.tfw",
            "DateString" : currentDateString
        }

        # Add object to list
        # Add the object to the return list.
        retList.append(currentObj)

        # Incremenet to next currentDateTime
        currentDateTime = currentDateTime +  datetime.timedelta(hours=3)
    return retList


# To get the accumulations, Just use,
# current_RastObj = rastObjList[n]
# current_tif_Location = current_RastObj['FTP_PathTo_TIF']
# current_tfw_Location = current_RastObj['FTP_PathTo_TFW']
# current_1Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.1day")
# current_1Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.1day")
# current_3Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.3day")
# current_3Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.3day")
# current_7Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.7day")
# current_7Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.7day")
def debug_Get_CompositLocations_From_Raster(currentRasterObj):
    current_tif_Location = currentRasterObj['FTP_PathTo_TIF']
    current_tfw_Location = currentRasterObj['FTP_PathTo_TFW']
    current_1Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.1day")
    current_1Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.1day")
    current_3Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.3day")
    current_3Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.3day")
    current_7Day_tif_Location = current_tif_Location.replace(".7.03hr",".7.7day")
    current_7Day_tfw_Location = current_tfw_Location.replace(".7.03hr",".7.7day")
    retObj = {
        "current_tif_Location":current_tif_Location,
        "current_tfw_Location":current_tfw_Location,
        "current_1Day_tif_Location":current_1Day_tif_Location,
        "current_1Day_tfw_Location":current_1Day_tfw_Location,
        "current_3Day_tif_Location":current_3Day_tif_Location,
        "current_3Day_tfw_Location":current_3Day_tfw_Location,
        "current_7Day_tif_Location":current_7Day_tif_Location,
        "current_7Day_tfw_Location":current_7Day_tfw_Location
    }
    return retObj



# Returns a date time which has a new hour value (Meant for standardizing the hours to 3 hour increments
def Extract_Support_Set_DateToStandard_3_Hour(hourValue, theDateTime):
    formatString = "%Y%m%d%H"
    newDateTimeString = theDateTime.strftime("%Y%m%d")
    if hourValue < 10:
        newDateTimeString += "0"
    newDateTimeString += str(hourValue)

    newDateTime = datetime.datetime.strptime(newDateTimeString, formatString)

    return newDateTime

# Get next 3 hour value from current hour.
def Extract_Support_Get_Next_3_Hour(currentHour):
    hourToReturn = None
    if currentHour % 3 == 0:
        hourToReturn = currentHour
    elif currentHour % 3 == 1:
        hourToReturn = currentHour + 2
    else:
        hourToReturn = currentHour + 1

    if hourToReturn > 21:
        hourToReturn = 21

    return hourToReturn


def Extract_Do_Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, theExtractWorkspace):

    addToLog("Extract_Do_Extract_FTP: Started") # , True)

    # Move these to settings at the earliest opportunity!!
    the_FTP_Host = "trmmopen.gsfc.nasa.gov" #"198.118.195.58" #trmmopen.gsfc.nasa.gov"  #"ftp://trmmopen.gsfc.nasa.gov"
    the_FTP_SubFolderPath = "pub/gis"
    the_FTP_UserName = "anonymous" #
    the_FTP_UserPass = "anonymous" #
    root_FTP_Path = "ftp://" + str(the_FTP_Host) + "/" + the_FTP_SubFolderPath
    addToLog("&&&&&&&&&&"+root_FTP_Path)
    #addToLog("Extract_Do_Extract_FTP: ALERT A")

    ExtractList = []
    lastBaseRaster = ""
    lastFTPFolder = ""
    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0
    debugFileDownloadLimiter = 10000 #10000        # For debugging, set this to a low number

    # Get the Start / End datetimes
    startDateTime = datetime.datetime.strptime(startDateTime_str, dateFormat_String)
    endDateTime = datetime.datetime.strptime(endDateTime_str, dateFormat_String)

    # Adjust the Start and end dates so they are on 3 hour increments from each other
    # Start Date adjustment
    newStartHour = Extract_Support_Get_Next_3_Hour(startDateTime.hour)
    standardized_StartDate = Extract_Support_Set_DateToStandard_3_Hour(newStartHour, startDateTime)

    # End Date adjustment
    newEndHour = Extract_Support_Get_Next_3_Hour(endDateTime.hour)
    standardized_EndDate = Extract_Support_Set_DateToStandard_3_Hour(newEndHour, endDateTime)

    # get a list of all the files within the start and end date
    expected_FilePath_Objects_To_Extract_WithinRange = Extract_Support_Get_Expected_FTP_Paths_From_DateRange(standardized_StartDate, standardized_EndDate, root_FTP_Path, the_FTP_SubFolderPath)
    addToLog("Extract_Do_Extract_FTP: expected_FilePath_Objects_To_Extract_WithinRange (list to process) " + str(expected_FilePath_Objects_To_Extract_WithinRange) , True)

    numFound = len(expected_FilePath_Objects_To_Extract_WithinRange)
    if numFound == 0:
        if startDateTime_str == endDateTime_str:
            addToLog("Extract_Do_Extract_FTP: ERROR: No files found for the date string "+startDateTime_str)
        else:
            addToLog("Extract_Do_Extract_FTP: ERROR: No files found between "+startDateTime_str+" and "+endDateTime_str)
    else:

        # Connect to FTP Server
        try:

            # QUICK REFACTOR NOTE: Something very strange was happening with the FTP and there isn't time to debug this issue.. going with URL Download instead for now.

            addToLog("Extract_Do_Extract_FTP: Connecting to FTP", True)
            # ftp_Connection = ftplib.FTP("trmmopen.gsfc.nasa.gov","anonymous","anonymous")
            ftp_Connection = ftplib.FTP(the_FTP_Host,the_FTP_UserName,the_FTP_UserPass)
            time.sleep(1)

            addToLog("Extract_Do_Extract_FTP: Downloading TIF and TFW files for each raster", True)

            # Holding information for the last FTP folder we changed to.
            lastFolder = ""

            # Iterate through each key file path and and perform the extraction.
            for curr_FilePath_Object in expected_FilePath_Objects_To_Extract_WithinRange:

                if counter_FilesDownloaded < debugFileDownloadLimiter:
                    addToLog("Extract_Do_Extract_FTP: DEBUG curr_FilePath_Object : " + str(curr_FilePath_Object), True)
                    # FTP, Change to folder,
                    currFTPFolder = curr_FilePath_Object['FTPSubFolderPath'] # + "/"      # FTP Change Directory requires slash at the end.

                    # Only change folders if we need to.
                    if currFTPFolder == lastFolder:
                        # Do nothing

                        pass
                    else:
                        time.sleep(1)
						
                        addToLog("Extract_Do_Extract_FTP: FTP, Changing folder to : " + str(currFTPFolder), True)
                        addToLog("********b4 cwd**********"+currFTPFolder)
                        ftp_Connection.cwd("/" + currFTPFolder)
                        addToLog("********a4cwd**********")
						
                        time.sleep(1)

                    lastFolder = currFTPFolder
                    try:
                        # Attempt to download the TIF and World File (Tfw)
                        addToLog("********try**********")

                        Tif_file_to_download = curr_FilePath_Object['TIF_3Hr_FileName']
                        downloadedFile_TIF = os.path.join(theExtractWorkspace,Tif_file_to_download)
                        addToLog("*********b4**********")

                        with open(downloadedFile_TIF, "wb") as f:
                            ftp_Connection.retrbinary("RETR %s" % Tif_file_to_download, f.write)

                        time.sleep(1)
                        addToLog("********atr***********")
                        Twf_file_to_download = curr_FilePath_Object['TWF_3Hr_FileName']
                        downloadedFile_TFW = os.path.join(theExtractWorkspace,Twf_file_to_download)
                        with open(downloadedFile_TFW, "wb") as f:
                            ftp_Connection.retrbinary("RETR %s" % Twf_file_to_download, f.write)

                        time.sleep(1)

                        # Two files were downloaed (or 'extracted') but we really only need a reference to 1 file (thats what the transform expects).. and Arc actually understands the association between the TIF and TWF files automatically
                        extractedFileList = []
                        extractedFileList.append(downloadedFile_TIF)
                        current_Extracted_Obj = {
                                'DateString' : curr_FilePath_Object['DateString'],
                                'Downloaded_FilePath' : downloadedFile_TIF,
                                'ExtractedFilesList' : convert_Obj_To_List(extractedFileList),
                                'downloadURL' : curr_FilePath_Object['FTP_PathTo_TIF'], #currentURL_ToDownload
                                'FTP_DataObj' : curr_FilePath_Object
                            }

                        ExtractList.append(current_Extracted_Obj)
                        lastBaseRaster = curr_FilePath_Object['BaseRasterName']
                        lastFTPFolder = curr_FilePath_Object['FTPSubFolderPath']
                        counter_FilesDownloaded += 1

                    except:
                        # If the raster file is missing or an error occurs during transfer..
                        addToLog("Extract_Do_Extract_FTP: ERROR.  Error downloading current raster " +  str(curr_FilePath_Object['BaseRasterName']))


        except:
            e = sys.exc_info()[0]
            errMsg = "Extract_Do_Extract_FTP: ERROR: Could not connect to FTP Server, Error Message: " + str(e)
            addToLog(errMsg)

    ret_ExtractObj = {
        'StartDateTime':startDateTime,
        'EndDateTime': endDateTime,
        'ExtractList':ExtractList,
        'lastBaseRaster' : lastBaseRaster,
        'lastFTPFolder' : lastFTPFolder
    }
    return ret_ExtractObj

def Extract_Controller_Method(ETL_TransportObject):

    # Check the setup for errors as we go.
    IsError = False
    ErrorMessage = ""

    # Get inputs for the next function


    # Inputs from ETL_TransportObject['SettingsObj']
    try:
        the_FileExtension = ETL_TransportObject['SettingsObj']['Download_File_Extension'] # TRMM_FileExtension # TRMM_File_Extension
        s3BucketRootPath = ETL_TransportObject['SettingsObj']['s3_BucketRootPath']
        s3AccessKey = ETL_TransportObject['SettingsObj']['s3_AccessKeyID']
        s3SecretKey = ETL_TransportObject['SettingsObj']['s3_SecretAccessKey']
        s3BucketName = ETL_TransportObject['SettingsObj']['s3_BucketName']
        s3PathTo_Files = ETL_TransportObject['SettingsObj']['s3_PathTo_TRMM_Files']
        s3_Is_Use_Local_IAM_Role = get_BoolSetting(ETL_TransportObject['SettingsObj']['s3_UseLocal_IAM_Role'])
        regEx_String = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
        dateFormat_String = ETL_TransportObject['SettingsObj']['Python_DateFormat']
        extractWorkspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Extract']
    except:
        e = sys.exc_info()[0]
        errMsg = "Extract_Controller_Method: ERROR: Could not get extract inputs, Error Message: " + str(e)
        addToLog(errMsg)
        IsError = True
        ErrorMessage += "|  " + errMsg


    # Get the Start and End Dates
    try:
        varList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
        GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace']
        mosaicName = varList[0]['mosaic_name'] # ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
        primaryDateField = varList[0]['primary_date_field']
        mosaicDS = os.path.join(GeoDB_Workspace,mosaicName)
        startDateTime = Extract_Support_GetStartDate(primaryDateField,mosaicDS)
        endDateTime = Extract_Support_GetEndDate()
        startDateTime_str = startDateTime.strftime(dateFormat_String)
        endDateTime_str = endDateTime.strftime(dateFormat_String)
    except:
        e = sys.exc_info()[0]
        errMsg = "Extract_Controller_Method: ERROR: Could not get Dates, Error Message: " + str(e)
        addToLog(errMsg)
        IsError = True
        ErrorMessage += "|  " + errMsg
    addToLog("Extract_Controller_Method: Using startDateTime_str : startDateTime : " + str(startDateTime_str) + " : " + str(startDateTime))
    addToLog("Extract_Controller_Method: Using endDateTime_str : endDateTime :  " + str(endDateTime_str) + " : " + str(endDateTime))

    # Execute the Extract Process.
    ExtractResult = Extract_Do_Extract_FTP(dateFormat_String, startDateTime_str, endDateTime_str, extractWorkspace)



    if len(ExtractResult['ExtractList']) == 0:
        IsError = True
        ErrorMessage += "|  Extract List contains 0 elements.  No files were extracted."
    # Package up items from the PreETL Step
    returnObj = {
        'ExtractResult': ExtractResult,
        'OldestDateTime': startDateTime,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }
    # Return the packaged items.
    return returnObj


#--------------------------------------------------------------------------
# Transform
#   The function(s) that perform the Transform step.
#   Typically, this involves reading from the list of extracted items and
#   performing some type of preperation on them to prepare for the Load step
#   This process may involve applying a projection to a raster, or an
#   XY Event layer to a vector set, or even transforming large images into
#   Smaller thumbnails and uploading them to a final location.
#   The last step of this process is usually to write data to the return object
#   with information about the transform step so that the Load step can
#   use that as an input.
#--------------------------------------------------------------------------

# See "Pre ETL" Section for the format of these functions
def Transform_ExampleSupportMethod():
    pass

# Copy rasters from their scratch location to their final location.
# Called for each extracted item
def Transform_Do_Transform_CopyRaster(coor_system, extractResultObj, varList, dateSTR, extFileList, rasterOutputLocation, colorMapLocation):
    # Blank output list
    outputVarFileList = []

    # Execute Transform Raster Copy
    try:
        # The way this is set up is if a single zip contains multiple files.. for TRMM, there is only a single file in the zip..
        # Keeping the code as it is, so it can be flexible to handle other cases in the future.
        for varDict in varList:
            varName = varDict["variable_name"]
            filePrefix = varDict["file_prefix"]
            fileSuffix = varDict["file_suffix"]
            mosaicName = varDict["mosaic_name"]
            primaryDateField = varDict["primary_date_field"]

            # Build the name of the raster file we're looking for based on
            #   the configuration for the variable and find it in the list
            #   of files that were extracted
            raster_base_name = filePrefix + dateSTR + fileSuffix
            # Find the file in the list of downloaded files associated with
            #   the current variable
            raster_file = ""
            raster_name = ""
            for aName in extFileList:
                currBaseName = os.path.basename(aName)
                if currBaseName == raster_base_name:
                    raster_file = aName
                    raster_name = raster_base_name

            # If we don't find the file in the list of downloaded files,
            #   skip this variable and move on; otherwise, process the file
            if len(raster_file) == 0:
                addToLog("Transform_Do_Transform_CopyRaster No file found for expected raster_base_name, " + str(raster_base_name) + "...skipping...", True)
            else:


                # Add the output raster location for the full raster path
                out_raster = os.path.join(rasterOutputLocation, raster_name)
                # Perform the actual conversion (If the file already exists, this process breaks.)
                if not arcpy.Exists(out_raster):
                    arcpy.CopyRaster_management(raster_file, out_raster)    # This operation DOES overwrite an existing file (so forecast items get overwritten by actual items when this process happens)
                    addToLog("Transform_Do_Transform_CopyRaster: Copied "+ os.path.basename(raster_file)+" to "+str(out_raster), True)
                else:
                    addToLog("Transform_Do_Transform_CopyRaster: Raster, "+ os.path.basename(raster_file)+" already exists at output location of: "+str(out_raster), True)

                # Apply a color map
                try:
                    arcpy.AddColormap_management(out_raster, "#", colorMapLocation)
                    addToLog("Transform_Do_Transform_CopyRaster: Color Map has been applied to "+str(out_raster), True)
                except:
                    addToLog("Transform_Do_Transform_CopyRaster: Error Applying color map to raster : " + str(out_raster) + " ArcPy Error Message: " + str(arcpy.GetMessages()))

                # Define the coordinate system
                sr = arcpy.SpatialReference(coor_system)
                arcpy.DefineProjection_management(out_raster, sr)
                addToLog("Transform_Do_Transform_CopyRaster: Defined coordinate system: "+ str(sr.name), True)
                # Append the output file and it's associated variable to the
                #   list of files processed
                currRastObj = {
                    "out_raster_file_location":out_raster,
                    "mosaic_ds_name":mosaicName,
                    "primary_date_field":primaryDateField
                }
                outputVarFileList.append(currRastObj)

    except:
        e = sys.exc_info()[0]
        addToLog("Transform_Do_Transform_CopyRaster: ERROR: Something went wrong during the transform process, Error Message: " + str(e))

    # Return the output list
    return outputVarFileList

def Transform_Controller_Method(ETL_TransportObject):
    # Do a "Transform" Process

    # Gather inputs
    coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']
    extractResultObj = ETL_TransportObject['Extract_Object']['ResultsObject']
    varList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
    rasterOutputLocation = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    colorMapLocation = ETL_TransportObject['SettingsObj']['trmm3Hour_ColorMapLocation']

    # For each item in the extract list.. call this function
    TransformResult_List = []
    current_ExtractList = ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['ExtractList']
    for currentExtractItem in current_ExtractList:
        current_dateSTR = currentExtractItem['DateString']
        current_extFileList = currentExtractItem['ExtractedFilesList']

        Transformed_File_List = Transform_Do_Transform_CopyRaster(coor_system, extractResultObj, varList, current_dateSTR, current_extFileList, rasterOutputLocation, colorMapLocation)
        if len(Transformed_File_List) == 0:
            # do nothing, no data returned
            pass
        else:
            CurrentTransObj = {
                'Transformed_File_List':Transformed_File_List,
                'date_string':current_dateSTR
            }
            TransformResult_List.append(CurrentTransObj)


    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    if len(TransformResult_List) == 0:
        IsError = True
        ErrorMessage += "|  Transform List contains 0 elements.  No files were transformed."
    # Package up items from the PreETL Step
    returnObj = {
        'TransformResult_List': TransformResult_List,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj


#--------------------------------------------------------------------------
# Load
#   The function(s) that perform the Load step.
#   Typically, this involves reading from the list of transformed items and
#   performing an operation that results in the transformed data being loaded
#   into arcgis (via geodatabase, enterprise database, sde connection, etc)
#   The last step of this process is usually to return information about
#   the results loaded and/or errors occured.
#   (Example, return number of items loaded)
#   If use in post etl or reporting is required, a list of items could be
#   returned simillar to how the extract and transform steps return.
#--------------------------------------------------------------------------

# See "Pre ETL" Section for the format of these functions
def Load_ExampleSupportMethod():
    pass



def Load_Do_Load_TRMM_Dataset(transFileList, geoDB_MosaicDataset_Workspace, regExp_Pattern, date_Format, coor_system):


    mdWS = geoDB_MosaicDataset_Workspace
    # Load each raster into its appropriate mosaic dataset
    numLoaded = 0

    for fileDict in transFileList:
        rasterFile = fileDict["out_raster_file_location"]       # Filesystem folder that holds raster files.
        rasterName = os.path.basename(rasterFile).replace(".tif","")     # different filename schema uses this -->  # os.path.basename(rasterFile).split(".")[0]
        addToLog("Load_Do_Load_TRMM_Dataset: rasterName " + str(rasterName), True)
        mosaicDSName = fileDict["mosaic_ds_name"]
        primaryDateField = fileDict["primary_date_field"]
        mosaicDS = os.path.join(mdWS, mosaicDSName)             # GeoDB/DatasetName

        addError = False

        # For now, skip the file if the mosaic dataset doesn't exist.  Could
        #   be updated to create the mosaic dataset if it's missing
        if not arcpy.Exists(mosaicDS):
            addToLog("Load_Do_Load_TRMM_Dataset: Mosaic dataset "+str(mosaicDSName)+", located at, " +str(mosaicDS)+" does not exist.  Skipping "+os.path.basename(rasterFile))
        else:
            try:
                # Add raster to mosaic dataset
                addError = False
                # Having some issues with the calculate statistics function...
                # having issues with raster not showing up on arc map or arc catalog.. exploring the settings here
                # spatialRef param was set to "#" before..  # Changed "EXCLUDE_DUPLICATES" to "OVERWRITE_DUPLICATES", Changed LAST Param from "#" to "FORCE_SPATIAL_REFERENCE"
                sr = arcpy.SpatialReference(coor_system)
                arcpy.AddRastersToMosaicDataset_management(mosaicDS, "Raster Dataset", rasterFile,\
                                                           "UPDATE_CELL_SIZES", "UPDATE_BOUNDARY", "NO_OVERVIEWS",\
                                                           "2", "#", "#", "#", "#", "NO_SUBFOLDERS",\
                                                           "EXCLUDE_DUPLICATES", "BUILD_PYRAMIDS", "CALCULATE_STATISTICS",\
                                                           "NO_THUMBNAILS", "Add Raster Datasets","#")
                addToLog("Load_Do_Load_TRMM_Dataset: Added " +str(rasterFile)+" to mosaic dataset "+str(mosaicDSName), True)
                numLoaded += 1


            except:
                e = sys.exc_info()[0]
                addToLog("Load_Do_Load_TRMM_Dataset: ERROR: Something went wrong when adding the raster to the mosaic dataset. Error Message: " + str(e) + " ArcPy Messages: " + str(arcpy.GetMessages(2)))
                addError = True

            if not addError:
                # Calculate statistics on the mosaic dataset
                try:
                    arcpy.CalculateStatistics_management(mosaicDS,1,1,"#","SKIP_EXISTING","#")
                    addToLog("Load_Do_Load_TRMM_Dataset: Calculated statistics on mosaic dataset "+str(mosaicDSName), True)
                    pass

                # Handle errors for calc statistics
                except:
                    e = sys.exc_info()[0]
                    addToLog("Load_Do_Load_TRMM_Dataset: ERROR: Error calculating statistics on mosaic dataset "+str(mosaicDSName)+"  Error Message: " + str(e) + " ArcPy Messages: " + str(arcpy.GetMessages(2)))
                    pass

                # Build attribute and value lists
                attrNameList = []
                attrExprList = []

                # Build a list of attribute names and expressions to use with
                #   the ArcPy Data Access Module cursor below
                HC_AttrName = "timestamp"
                CurrentDateTime = Extract_Support_Get_PyDateTime_From_String(rasterName, regExp_Pattern, date_Format)
                attrNameList.append(HC_AttrName)
                attrExprList.append(CurrentDateTime)


                # Quick fix, Adding the extra fields
                # 'start_datetime' and 'end_datetime'
                CurrentDateTime_Minus_1hr30min = CurrentDateTime - datetime.timedelta(hours=1.5)
                attrNameList.append('start_datetime')
                attrExprList.append(CurrentDateTime_Minus_1hr30min)
                CurrentDateTime_Plus_1hr30min = CurrentDateTime + datetime.timedelta(hours=1.5)
                attrNameList.append('end_datetime')
                attrExprList.append(CurrentDateTime_Plus_1hr30min)

                # Update the attributes with their configured expressions
                #   (ArcPy Data Access Module UpdateCursor)
                try:
                    wClause = arcpy.AddFieldDelimiters(mosaicDS,"name")+" = '"+rasterName+"'"
                    with arcpy.da.UpdateCursor(mosaicDS, attrNameList, wClause) as cursor:
                        for row in cursor:
                            for idx in range(len(attrNameList)):
                                #row[idx] = eval(attrExprList[idx]) # Not evaling this right now, using an actual value
                                row[idx] = attrExprList[idx]

                            cursor.updateRow(row)

                    addToLog("Load_Do_Load_TRMM_Dataset: Calculated attributes for raster", True)
                    del cursor

                # Handle errors for calculating attributes
                except:
                    e = sys.exc_info()[0]
                    addToLog("Load_Do_Load_TRMM_Dataset: ERROR: Error calculating attributes for raster"+str(rasterFile)+"  Error Message: " + str(e))


    retObj = {
        'NumberLoaded': numLoaded
    }

    return retObj

def Load_Controller_Method(ETL_TransportObject):
    # Do a "Load" Process



    # Gather inputs
    GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace'] #['SettingsObj']['Raster_Final_Output_Location']
    theRegEx = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
    theDateFormat = ETL_TransportObject['SettingsObj']['Python_DateFormat']
    coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']

    # For each item in the Transform list.. call this function
    LoadResult_List = []
    current_TransformList = ETL_TransportObject['Transform_Object']['ResultsObject']['TransformResult_List']
    for currentTransformItem in current_TransformList:
        current_TransFileList = currentTransformItem['Transformed_File_List'] # transFileList

        current_LoadResultObj = Load_Do_Load_TRMM_Dataset(current_TransFileList, GeoDB_Workspace, theRegEx, theDateFormat, coor_system)
        LoadResult_List.append(current_LoadResultObj)

    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    if len(LoadResult_List) == 0:
        IsError = True
        ErrorMessage += "|  Load List contains 0 elements.  No items were Loaded."


    # Package up items from the PreETL Step
    returnObj = {
        'LoadResult_List': LoadResult_List,
        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj

#--------------------------------------------------------------------------
# Post ETL
#   Processes that must be performed after the ETL process.
#   This may also include operations on the data which are independent of the
#   ETL process.  For example, CREST's insert line items to seperate postgres
#   DB operations.
#--------------------------------------------------------------------------

# See "Pre ETL" Section for the format of these functions
def PostETL_ExampleSupportMethod():
    pass

# Refresh the list of Permissions
def PostETL_RefreshPermissions_For_Accumulations(pathToGeoDB, rasterDatasetList):
    for dataSetName in rasterDatasetList:
        try:
            mds = os.path.join(pathToGeoDB, dataSetName)
            arcpy.ChangePrivileges_management(mds,"role_servir_editor","GRANT","GRANT")
            addToLog("PostETL_RefreshPermissions_For_Accumulations: Editor Permissions set for " + str(dataSetName) + ", arcpy Message: " + str(arcpy.GetMessages()))
            arcpy.ChangePrivileges_management(mds,"role_servir_viewer","GRANT","#")
            addToLog("PostETL_RefreshPermissions_For_Accumulations: Viewer Permissions set for " + str(dataSetName) + ", arcpy Message: " + str(arcpy.GetMessages()))
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_RefreshPermissions_For_Accumulations: ERROR, Something went wrong when setting permissions.  System Error Message: "+ str(e) + ", ArcPy Message: " + str(arcpy.GetMessages()))

# lastRasterName # Expecting something like : "3B42RT.2014062509.7.03hr"
# whichComposite # Expecting something like : "1day" , "3day", "7day"
# ftpSubfolder # Expecting something like : "/pub/gis/201406"

def PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN(lastRasterName, whichComposite, ftpSubfolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, rasterDataSetName):
    #addToLog("CUSTOM RASTERS SUB:  Alert A")
    # filter input
    if whichComposite == "1day":
        pass
    elif whichComposite == "3day":
        pass
    elif whichComposite == "7day":
        pass
    else:
        addToLog("PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN: Bad input value for 'whichComposite' : " + str(whichComposite) + ", bailing out!")
        return


    # Pre set up, Create Expected filenames.
    newBaseName = lastRasterName.replace("03hr", whichComposite)
    TIF_FileName = newBaseName + ".tif"
    TFW_FileName = newBaseName + ".tfw"
    location_ToSave_TIF_File = os.path.join(scratchFolder,TIF_FileName)
    location_ToSave_TFW_File = os.path.join(scratchFolder,TFW_FileName)
    subTransformScratchFolder = os.path.join(scratchFolder,whichComposite)
    trans_Raster_File = os.path.join(subTransformScratchFolder,TIF_FileName)

    # Create Temp Subfolder
    try:
        make_And_Validate_Folder(subTransformScratchFolder)
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN: ERROR, Something went wrong when creating the sub scratch folder.  System Error Message: "+ str(e))


    # Connect to FTP, download the files  # TRMMs ftp acts funny if we don't enter delays.. thats why using time.sleep(1)
    time.sleep(1)
    ftp_Connection = ftplib.FTP(ftpParams['ftpHost'],ftpParams['ftpUserName'],ftpParams['ftpUserPass'])
    time.sleep(1)

    # Change Folder FTP
    # Extra ftpSubfolder
    ftp_Connection.cwd(ftpSubfolder)
    time.sleep(1)
    # Download the TIF and World Files
    with open(location_ToSave_TIF_File, "wb") as f:
        ftp_Connection.retrbinary("RETR %s" % TIF_FileName, f.write)

    time.sleep(1)

    with open(location_ToSave_TFW_File, "wb") as f:
        ftp_Connection.retrbinary("RETR %s" % TFW_FileName, f.write)
		
    time.sleep(1)
	
    ftp_Connection.close()

    # Apply Transform (Spatial Projection)
    # Copy Raster
    if arcpy.Exists(trans_Raster_File):
        # Do nothing, raster already exists at location
        pass
    else:
        arcpy.CopyRaster_management(location_ToSave_TIF_File, trans_Raster_File)

    # Apply Spatial Reference
    sr = arcpy.SpatialReference(coor_system)

    #addToLog("CUSTOM RASTERS SUB:  Alert N  ======== sr : " + str(sr))

    arcpy.DefineProjection_management(trans_Raster_File, sr)

    #addToLog("CUSTOM RASTERS SUB:  Alert O")

    # Add Raster to raster catalog
    # Add Raster to the Geodatbase  # pathToGeoDB, rasterDataSetName
    path_To_RasterDestination = os.path.join(pathToGeoDB, rasterDataSetName)

    # Delete the old one if it exists first
    if arcpy.Exists(path_To_RasterDestination):
        addToLog("PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN: Deleting... " + str(path_To_RasterDestination))
        r = arcpy.Delete_management(path_To_RasterDestination)
        addToLog("PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN:Delete_management Result " + str(r.status))
    arcpy.CopyRaster_management(trans_Raster_File, path_To_RasterDestination)

def PostETL_Support_Build_Custom_Rasters(PostETL_CustomRaster_Params, ETL_TransportObject):

    # Gather params
    fileFolder_With_TRMM_Rasters = PostETL_CustomRaster_Params['fileFolder_With_TRMM_Rasters'] # r"C:\ksArcPy\trmm\rastout" # Settings, 'Raster_Final_Output_Location'
    color_map = PostETL_CustomRaster_Params['color_map'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_3hour.clr" # PLACEHOLDER
    output_basepath = PostETL_CustomRaster_Params['output_basepath'] # "C:\\kris\\!!Work\\ETL_TRMM\\GeoDB\\TRMM.gdb"
    raster_catalog_fullpath = PostETL_CustomRaster_Params['raster_catalog_fullpath'] # output_basepath + "\\TRMM"
    raster_catalog_options_datetime_field = PostETL_CustomRaster_Params['raster_catalog_options_datetime_field'] # "timestamp"  #"datetime"
    raster_catalog_options_datetime_sql_cast = PostETL_CustomRaster_Params['raster_catalog_options_datetime_sql_cast'] # "date"
    raster_catalog_options_datetime_field_format = PostETL_CustomRaster_Params['raster_catalog_options_datetime_field_format'] # "%Y-%m-%d %H:00:00" # Query_DateFormat>%Y-%m-%d %H:00:00 # "%m-%d-%Y %I:%M:%S %p"
    start_datetime = PostETL_CustomRaster_Params['start_datetime'] # datetime.datetime.utcnow()
    trmm1Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm1Day_RasterCatalogName'] # "TRMM1Day"
    trmm7Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm7Day_RasterCatalogName'] # "TRMM7Day"
    trmm30Day_RasterCatalogName = PostETL_CustomRaster_Params['trmm30Day_RasterCatalogName'] # "TRMM30Day"
    trmm1Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm1Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_1day.clr"
    trmm7Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm7Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_7day.clr"
    trmm30Day_ColorMapLocation = PostETL_CustomRaster_Params['trmm30Day_ColorMapLocation'] # r"C:\kris\!!Work\ETL_TRMM\SupportFiles\TRMM_30Day.clr"
    workSpacePath = PostETL_CustomRaster_Params['workSpacePath'] # r"C:\kris\!!Work\ETL_TRMM\ScratchWorkspace\custom_RenameLater"

    # 'clip_extent' <str>: the processing extent contained within "-180.0 -50.0 180.0 50.0"
    # initialize request config objects -------------------------------------
    factory_specifications = {

        "AddColormap_management_config": { # optional, comment out/delete entire key if no color map is needed
            "input_CLR_file":color_map
        },
        "CopyRaster_management_config":{
            'config_keyword':'',
            'background_value':'',
            'nodata_value':'',
            'onebit_to_eightbit':'',
            'colormap_to_RGB':'',
            'pixel_type':'16_BIT_UNSIGNED'
        }
    }
    input_raster_catalog_options = {

        'raster_catalog_fullpath': raster_catalog_fullpath,  # raster_catalog.fullpath,
        "raster_name_field":'Name',
        "datetime_field":raster_catalog_options_datetime_field,                 #raster_catalog.options['datetime_field'],                  # Original Val "datetime"
        'datetime_sql_cast':raster_catalog_options_datetime_sql_cast,           # raster_catalog.options['datetime_sql_cast'],              # Original Val "date"
        'datetime_field_format':raster_catalog_options_datetime_field_format,    # raster_catalog.options['datetime_field_format'],          # Original Val "%m-%d-%Y %I:%M:%S %p"
        'start_datetime':start_datetime
    }

    # TRMM1Day config --------------------------------------------------------------------------------
    factory_specifications_1day = deepcopy(factory_specifications)
    factory_specifications_1day['output_raster_fullpath'] = os.path.join(output_basepath, trmm1Day_RasterCatalogName) #"TRMM1Day")
    factory_specifications_1day['AddColormap_management_config']['input_CLR_file'] = trmm1Day_ColorMapLocation # "D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_1day.clr"
    input_raster_catalog_options_1day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_1day['end_datetime'] = start_datetime - datetime.timedelta(days=1)
    trmm_1day = TRMMCustomRasterRequest({

        'factory_specifications':factory_specifications_1day,
        'input_raster_catalog_options':input_raster_catalog_options_1day
    })

    # TRMM7Day config --------------------------------------------------------------------------------
    factory_specifications_7day = deepcopy(factory_specifications)
    factory_specifications_7day['output_raster_fullpath'] = os.path.join(output_basepath, trmm7Day_RasterCatalogName) #"TRMM7Day")
    factory_specifications_7day['AddColormap_management_config']['input_CLR_file'] = trmm7Day_ColorMapLocation #"D:\\SERVIR\\ReferenceNode\\MapServices\\trmm_7day.clr"
    input_raster_catalog_options_7day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_7day['end_datetime'] = start_datetime - datetime.timedelta(days=7)
    trmm_7day = TRMMCustomRasterRequest({

        'factory_specifications':factory_specifications_7day,
        'input_raster_catalog_options':input_raster_catalog_options_7day
    })

    # TRMM30Day config --------------------------------------------------------------------------------
    factory_specifications_30day = deepcopy(factory_specifications)
    factory_specifications_30day['output_raster_fullpath'] = os.path.join(output_basepath, trmm30Day_RasterCatalogName) #"TRMM30Day")
    factory_specifications_30day['AddColormap_management_config']['input_CLR_file'] = trmm30Day_ColorMapLocation #"D:\\SERVIR\\ReferenceNode\\MapServices\\TRMM_30Day.clr"
    input_raster_catalog_options_30day = deepcopy(input_raster_catalog_options)
    input_raster_catalog_options_30day['end_datetime'] = start_datetime - datetime.timedelta(days=30)
    trmm_30day = TRMMCustomRasterRequest({

        'factory_specifications':factory_specifications_30day,
        'input_raster_catalog_options':input_raster_catalog_options_30day
    })

    # initialize object responsible for creating the TRMM composities
    trmm_custom_raster_factory = TRMMCustomRasterCreator({

        'workspace_fullpath': workSpacePath, #os.path.join(sys.path[0], "TRMMCustomRasters"),
        'remove_all_rasters_on_finish':False,
        'archive_options': {
            'raster_name_prefix':"t_", # identify rasters to delete by this prefix
            'local_raster_archive_days':30, # only keep rasters local within this many days
            'raster_name_datetime_format':"t_%Y%m%d%H" # format of rasters to create a datetime object
        },
        'fileFolder_With_TRMM_Rasters' : fileFolder_With_TRMM_Rasters,
        'debug_logger':addToLog,
        'exception_handler':addToLog #exception_manager.handleException
    })

    #trmm_custom_raster_factory.addCustomRasterReuests([trmm_1day, trmm_7day, trmm_30day])

    trmm_custom_raster_factory.addCustomRasterReuests([trmm_30day]) # We only want to create the 30 day one, the 1, and 7 day can be downloaded.
    trmm_custom_raster_factory.createCustomRasters() # start the composite creation process


    # And for the 1, 3, and 7 day.. download them from the source and upload them.
    try:
        addToLog("CUSTOM RASTERS:  ALERT 1 ")
        # FTP Info
        ftpParams = {
            "ftpHost" : "trmmopen.gsfc.nasa.gov",
            "ftpUserName" : "anonymous",
            "ftpUserPass" : "anonymous"
        }

        lastRasterName = ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['lastBaseRaster']
        lastFTPSubFolder = "/" + str(ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['lastFTPFolder'])
        scratchFolder = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PostETL']
        coor_system = ETL_TransportObject['SettingsObj']['TRMM_RasterTransform_CoordSystem']
        pathToGeoDB = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace']

        try:
            PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN(lastRasterName, "1day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "TRMM1Day")
        except:
            e1 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 1day to TRMM1Day.  System Error Message: "+ str(e1))
        try:
            PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN(lastRasterName, "3day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "TRMM3Day")
        except:
            e3 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 3day to TRMM3Day.  System Error Message: "+ str(e3))
        try:
            PostETL_Download_And_Load_CustomRaster_From_TRMMOPEN(lastRasterName, "7day", lastFTPSubFolder, ftpParams, scratchFolder, coor_system, pathToGeoDB, "TRMM7Day")
        except:
            e7 = sys.exc_info()[0]
            addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom raster 7day to TRMM7Day.  System Error Message: "+ str(e7))
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Support_Build_Custom_Rasters: ERROR, Something went wrong when attempting to download and load custom rasters.  System Error Message: "+ str(e))


# Stops the TRMM services, runs the custom raster generation routine, then restarts the TRMM services
def PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, service_Options_List, ETL_TransportObject):

    # For each service, Stop them all
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to stop all TRMM related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        # Try and stop each service
        try:
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
            tokenResponseJSON = json.loads(tokenResponse)
            token = tokenResponseJSON["token"]

            # Attempt to stop the current service
            stopParams = urllib.urlencode({"token":token,"f":"json"})
            stopResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/stop?",stopParams).read()
            stopResponseJSON = json.loads(stopResponse)
            stopStatus = stopResponseJSON["status"]

            if stopStatus <> "success":
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Unable to stop service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+stopStatus)
            else:
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Service: " + str(current_ServiceName) + " has been stopped.")

        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Stop Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))



    # Run the code for creating custom rasters
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to update Custom Rasters")
    try:
        PostETL_Support_Build_Custom_Rasters(PostETL_CustomRaster_Params, ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Something went wrong while building TRMM Custom Rasters, System Error Message: "+ str(e))



    # For each service, Start them all
    addToLog("PostETL_Do_Update_Service_And_CustomRasters: About to restart all TRMM related services")
    for current_Service in service_Options_List:
        current_Description = current_Service['Description']
        current_AdminDirURL = current_Service['admin_dir_URL']
        current_Username = current_Service['username']
        current_Password = current_Service['password']
        current_FolderName = current_Service['folder_name']
        current_ServiceName = current_Service['service_name']
        current_ServiceType = current_Service['service_type']

        # Try and start each service
        try:
            # Get a token from the Administrator Directory
            tokenParams = urllib.urlencode({"f":"json","username":current_Username,"password":current_Password,"client":"requestip"})
            tokenResponse = urllib.urlopen(current_AdminDirURL+"/generateToken?",tokenParams).read()
            tokenResponseJSON = json.loads(tokenResponse)
            token = tokenResponseJSON["token"]

            # Attempt to stop the current service
            startParams = urllib.urlencode({"token":token,"f":"json"})
            startResponse = urllib.urlopen(current_AdminDirURL+"/services/"+current_FolderName+"/"+current_ServiceName+"."+current_ServiceType+"/start?",startParams).read()
            startResponseJSON = json.loads(startResponse)
            startStatus = startResponseJSON["status"]

            if startStatus == "success":
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Started service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType))
            else:
                addToLog("PostETL_Do_Update_Service_And_CustomRasters: Unable to start service "+str(current_FolderName)+"/"+str(current_ServiceName)+"/"+str(current_ServiceType)+" STATUS = "+startStatus)
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Do_Update_Service_And_CustomRasters: ERROR, Start Service failed for " + str(current_ServiceName) + ", System Error Message: "+ str(e))



def PostETL_Support_RemoveScratchFolders_Generic(folder):
    try:
        shutil.rmtree(folder)
    except:
        e = sys.exc_info()[0]
        addToLog("PostETL_Support_RemoveScratchFolders_Generic: ERROR: Error removing scratch folder "+str(folder)+" and its contents.  Please delete Manually.  System Error Message: " + str(e))

# Removes the Scratch Folders for etl processes.
def PostETL_Support_RemoveScratchFolders(pre, e, t, l, post):
    PostETL_Support_RemoveScratchFolders_Generic(pre)
    PostETL_Support_RemoveScratchFolders_Generic(e)
    PostETL_Support_RemoveScratchFolders_Generic(t)
    PostETL_Support_RemoveScratchFolders_Generic(l)
    PostETL_Support_RemoveScratchFolders_Generic(post)


def PostETL_Controller_Method(ETL_TransportObject):
    # Do a "PostETL" Process

    # Gathering inputs
    rasterOutputLocation = ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location']
    intervalString = ETL_TransportObject['SettingsObj']['TRMM_RasterArchiveDays']
    regExp_Pattern = ETL_TransportObject['SettingsObj']['RegEx_DateFilterString']
    rastDateFormat = ETL_TransportObject['SettingsObj']['Python_DateFormat']
    theVarList = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Variable_Dictionary_List']
    oldDate = Unsorted_GetOldestDate(intervalString) 

    GeoDB_Workspace = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['GeoDB_Dataset_Workspace']
    queryDateFormat = ETL_TransportObject['SettingsObj']['Query_DateFormat']


    # Inputs for the 3 Custom Raster generations.
    theOutputBasePath = GeoDB_Workspace #'C:\\kris\\!!Work\\ETL_TRMM\\GeoDB\\TRMM.gdb'
    PostETL_CustomRaster_Params = {
        'fileFolder_With_TRMM_Rasters' : ETL_TransportObject['SettingsObj']['Raster_Final_Output_Location'], # r'C:\ksArcPy\trmm\rastout',
        'color_map' : ETL_TransportObject['SettingsObj']['TRMM_ColorMapFile_3_Hour'],  # r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_3hour.clr',
        'output_basepath' : theOutputBasePath,
        'raster_catalog_fullpath' : theOutputBasePath + '\\' + theVarList[0]['mosaic_name'],  # \\TRMM', # Should be a setting  mosaic_name
        'raster_catalog_options_datetime_field' : theVarList[0]['primary_date_field'],  # 'timestamp',
        'raster_catalog_options_datetime_sql_cast' : 'date',
        'raster_catalog_options_datetime_field_format' : ETL_TransportObject['SettingsObj']['Query_DateFormat'],  # '%Y-%m-%d %H:00:00',
        'start_datetime' : datetime.datetime.utcnow(),
        'trmm1Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm1Day_RasterCatalogName'],  #  'TRMM1Day',
        'trmm7Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm7Day_RasterCatalogName'],  #  'TRMM7Day',
        'trmm30Day_RasterCatalogName' : ETL_TransportObject['SettingsObj']['trmm30Day_RasterCatalogName'],  #  'TRMM30Day',
        'trmm1Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm1Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_1day.clr',
        'trmm7Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm7Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\trmm_7day.clr',
        'trmm30Day_ColorMapLocation' : ETL_TransportObject['SettingsObj']['trmm30Day_ColorMapLocation'],  #  r'C:\kris\!!Work\ETL_TRMM\SupportFiles\TRMM_30Day.clr',
        'workSpacePath' : ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PostETL'] # r'C:\kris\!!Work\ETL_TRMM\ScratchWorkspace\custom_RenameLater'

    }
    pkl_file = open('config.pkl', 'rb')
    myConfig = pickle.load(pkl_file) #store the data from config.pkl file
    pkl_file.close()
    # service_Options_List
    TRMM_Service_Options = [{
        "Description":"TRMM Mosaic Dataset Service",
        "admin_dir_URL":myConfig['admin_dir_URL'],
        "username":myConfig['username'],
        "password":myConfig['password'],
        "folder_name":myConfig['folder_name'],
        "service_name":myConfig['service_name'],
        "service_type":myConfig['service_type']
    }]

    # Update Service and Build new static composits.
    if len(ETL_TransportObject['Load_Object']['ResultsObject']['LoadResult_List']) == 0:
        addToLog("PostETL_Controller_Method: No items were loaded, Service will not be stopped and restarted.  Custom rasters will not be generated.")
    else:
        try:
            PostETL_Do_Update_Service_And_Custom_Rasters(PostETL_CustomRaster_Params, TRMM_Service_Options, ETL_TransportObject)
            pass
        except:
            e = sys.exc_info()[0]
            addToLog("PostETL_Controller_Method: ERROR, something went wrong when trying to restart services and create the custom rasters.  System Error Message: "+ str(e))
  
    # Data Clean up
    if oldDate == None:
        # do nothing, we don't have an a date to use to remove items..
        pass
    else:
        # Do the clean up stuff

        # Data Clean up - Remove old raster items from the geodatabase
        num_Of_Rasters_Removed_FromGeoDB = Unsorted_removeRastersMosaicDataset(theVarList, GeoDB_Workspace, oldDate, queryDateFormat)

        # Data Clean up - Remove old rasters from the file system
        # Don't remove any rasters from the file system if the last step failed..
        if num_Of_Rasters_Removed_FromGeoDB > 0:
            num_Of_Rasters_Deleted_FromFileSystem = Unsorted_dataCleanup(rasterOutputLocation, oldDate,regExp_Pattern,rastDateFormat)



    # Clean Scratch Workspaces
    folder_Pre = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PreETL']
    folder_E = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Extract']
    folder_T = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Transform']
    folder_L = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['Load']
    folder_Post = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']['Scratch_WorkSpace_Locations']['PostETL']
    # Comment the next line to keep the scratch items available for debugging.
    PostETL_Support_RemoveScratchFolders(folder_Pre, folder_E, folder_T, folder_L, folder_Post)
    addToLog("PostETL_Controller_Method: Scratch Workspaces should now be cleaned")


    # Refresh the list of Permissions for accumulation rasters
    rasterDatasetList = ["TRMM1Day","TRMM3Day", "TRMM7Day", "TRMM30Day"]
    PostETL_RefreshPermissions_For_Accumulations(GeoDB_Workspace, rasterDatasetList)

    # Check the above setup for errors
    IsError = False
    ErrorMessage = ""

    # Package up items from the PreETL Step
    returnObj = {

        'IsError': IsError,
        'ErrorMessage':ErrorMessage
    }

    # Return the packaged items.
    return returnObj


    return "PostETL_Controller_Method result object here"


#--------------------------------------------------------------------------
# Finalized Simple Log Report
#   This function checks for errors and various metrics of the ETL
#   process and outputs all that to the log near the end of code execution
#--------------------------------------------------------------------------

# Check for error, output the message if found.
def output_Error_For_ResultObj(resultObj, sectionName):
    try:
        if resultObj['IsError'] == True:
            errMsg = resultObj['ErrorMessage']
            addToLog(" === ERROR REPORT: "+str(sectionName)+":  " + str(errMsg))
        else:
            addToLog(" === REPORT: "+str(sectionName)+":  No errors to report.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Error_For_ResultObj: ERROR: Error displaying errors for: "+str(sectionName)+" System Error Message: " + str(e))



def output_Final_Log_Report(ETL_TransportObject):

    # Get report items and output them.

    # Extract
    try:
        numExtracted = len(ETL_TransportObject['Extract_Object']['ResultsObject']['ExtractResult']['ExtractList'])
        addToLog(" === REPORT: Extract: " + str(numExtracted) + " Items were extracted.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Extract report.  System Error Message: " + str(e))

    # Transform
    try:
        numTransformed = len(ETL_TransportObject['Transform_Object']['ResultsObject']['TransformResult_List'])
        addToLog(" === REPORT: Transform: " + str(numTransformed) + " Items were transformed.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Transform report.  System Error Message: " + str(e))

    # Load
    try:
        numLoaded = len(ETL_TransportObject['Load_Object']['ResultsObject']['LoadResult_List'])
        addToLog(" === REPORT: Load: " + str(numLoaded) + " Items were loaded.")
    except:
        e = sys.exc_info()[0]
        addToLog("output_Final_Log_Report: ERROR: Error outputing Load report.  System Error Message: " + str(e))


    # Errors for each step
    resultsObj_PreETL = ETL_TransportObject['Pre_ETL_Object']['ResultsObject']
    resultsObj_Extract = ETL_TransportObject['Extract_Object']['ResultsObject']
    resultsObj_Transform = ETL_TransportObject['Transform_Object']['ResultsObject']
    resultsObj_Load = ETL_TransportObject['Load_Object']['ResultsObject']
    resultsObj_PostETL = ETL_TransportObject['Post_ETL_Object']['ResultsObject']
    output_Error_For_ResultObj(resultsObj_PreETL, "Pre ETL")
    output_Error_For_ResultObj(resultsObj_Extract, "Extract")
    output_Error_For_ResultObj(resultsObj_Transform, "Transform")
    output_Error_For_ResultObj(resultsObj_Load, "Load")
    output_Error_For_ResultObj(resultsObj_PostETL, "Post ETL")

#--------------------------------------------------------------------------
# Controller
#   The function(s) which control code execution for the entire process
#   This area is where settings are loaded into objects and passed down
#   through the various ETL steps and functions.
#--------------------------------------------------------------------------
# Main Controller function for this script.
def main(config_Settings):

    # Get a start time for the entire script run process.
    time_TotalScriptRun_Process = get_NewStart_Time()

    # Clear way to show entry in the log file for a script session start
    addToLog("======================= SESSION START =======================")

    # Config Settings
    # Get a reference to the config settings object, particulary the node in the xml doc that contains nodes the script may be using.
    settingsObj = config_Settings.xmldict['ConfigObjectCollection']['ConfigObject']

    # Access to the Config settings example
    current_ScriptSession_Name =  settingsObj['Name']
    addToLog("Script Session Name is: " + current_ScriptSession_Name)

    # Set up Detailed Logging
    current_DetailedLogging_Setting = settingsObj['DetailedLogging']
    global g_DetailedLogging_Setting
    if current_DetailedLogging_Setting == '1':
        g_DetailedLogging_Setting = True
    else:
        g_DetailedLogging_Setting = False
    addToLog("Main: Detailed logging has been enabled", True)


    # Prep Objects and Logic
    # Insert code here for configuring the ETL Transport Object's items needed by any processes.
    # This area may be blank if there is no preconfig or setup to perform.
    VariableDictionaryList = settingsObj['VariableDictionaryList']

    # Create ETL_TransportObject
    # This object can be used to transport various items such as settings, or preconfigured objects to the various functions called by the controller.
    # The "ResultsObject" items are placeholders for processes which have not yet occured.
    # The "OtherItems" are placeholders for this template.  Basically replace those with what ever kind of info that is pulled in from the settings and/or coded into the specific script.  # in the Crest ETL, this may be a good place to put the 'variable' dictionaries for example.
    ETL_TransportObject = {
        "SettingsObj": settingsObj,
        "Pre_ETL_Object" : {"ResultsObject":None,"OtherItems":None}, #"This is a place holder for OtherItems"},
        "Extract_Object" : {"ResultsObject":None,"OtherItems":None},
        "Transform_Object" : {"ResultsObject":None,"OtherItems":None},
        "Load_Object" : {"ResultsObject":None,"OtherItems":None},
        "Post_ETL_Object" : {"ResultsObject":None,"OtherItems":None},
    }

    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before PreETL method call): " + str(ETL_TransportObject), True)

    # Execute Pre ETL, Log the Time, and load the Results object.
    time_PreETL_Process = get_NewStart_Time()
    try:
        addToLog("========= Pre ETL =========")
        ETL_TransportObject['Pre_ETL_Object']['ResultsObject'] = PreETL_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: PreETL ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_PreETL_Process : " + get_Elapsed_Time_As_String(time_PreETL_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Extract method call): " + str(ETL_TransportObject), True)

    # Execute Extract, Log the Time, and load the Results object.
    time_Extract_Process = get_NewStart_Time()
    try:
        addToLog("========= EXTRACTING =========")
        ETL_TransportObject['Extract_Object']['ResultsObject'] = Extract_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: EXTRACTING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Extract_Process : " + get_Elapsed_Time_As_String(time_Extract_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Transform method call): " + str(ETL_TransportObject), True)

    # Execute Transform, Log the Time, and load the Results object.
    time_Transform_Process = get_NewStart_Time()
    try:
        addToLog("========= TRANSFORMING =========")
        ETL_TransportObject['Transform_Object']['ResultsObject'] = Transform_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: TRANSFORMING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Transform_Process : " + get_Elapsed_Time_As_String(time_Transform_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before Load method call): " + str(ETL_TransportObject), True)

    # Execute Load, Log the Time, and load the Results object.
    time_Load_Process = get_NewStart_Time()
    try:
        addToLog("========= LOADING =========")
        ETL_TransportObject['Load_Object']['ResultsObject'] = Load_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: LOADING ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_Load_Process : " + get_Elapsed_Time_As_String(time_Load_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (Before PostETL method call): " + str(ETL_TransportObject), True)

    # Execute Post ETL, Log the Time, and load the Results object.
    time_PostETL_Process = get_NewStart_Time()
    try:
        addToLog("========= Post ETL =========")
        ETL_TransportObject['Post_ETL_Object']['ResultsObject'] = PostETL_Controller_Method(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: Post ETL ERROR, something went wrong, ERROR MESSAGE: "+ str(e))
    addToLog("TIME PERFORMANCE: time_PostETL_Process : " + get_Elapsed_Time_As_String(time_PostETL_Process))
    # Detailed log entry showing the current state of the ETL_TransportObject
    addToLog("main: Current State of ETL_TransportObject (After PostETL method call): " + str(ETL_TransportObject), True)

    # Echo Errors to the log
    try:
        output_Final_Log_Report(ETL_TransportObject)
    except:
        e = sys.exc_info()[0]
        addToLog("main: Error Reporting Errors!, something went wrong, ERROR MESSAGE: "+ str(e))

    # Add a log entry showing the amount of time the script ran.
    # Note: It may be good practice to use a common phrase such as "TIME PERFORMANCE" to make it easier to search log files for the performance details since the log files can end up generating a lot of text.
    addToLog("TIME PERFORMANCE: time_TotalScriptRun_Process : " + get_Elapsed_Time_As_String(time_TotalScriptRun_Process))

    # Clear way to show entry in the log file for a script session end
    addToLog("======================= SESSION END =======================")
    # END


#--------------------------------------------------------------------------
# Entry Point
#--------------------------------------------------------------------------
main(g_ConfigSettings)
# END