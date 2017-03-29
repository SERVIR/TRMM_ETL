#-------------------------------------------------------------------------------
# Name:        ks_AdaptedLogger.py
# Purpose:     Adapted Logger, Basically taking the spatial dev logger
#               and adapting it to be used by this script.
#
# Author:      Kris Stanton
#
# Created:     01/27/2014 (mm/dd/yyyy)
# Copyright:   (c) kstanto1 2014
# Licence:     <your licence>
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------



# And the logger needs these..
import os

from datetime import datetime, timedelta

import logging
from logging.handlers import RotatingFileHandler






# The Logger..
class ETLDebugLogger(object):

    """
        constructor arguments:

            debug_log_basename <str>: name of the debug log
            debug_log_dir <str>: output directory for debug logs
            debug_log_options <dict>:

                'log_datetime_format' <str>: datetime format for debug logs
                'log_file_extn' <str>: extension of debug logs
                'debug_log_archive_days' <int>: number of days to keep debug logs

        fields:

            log_datetime_format: see above
            log_file_extn: see above
            debug_log_archive_days: see above
            debug_log_name <str>: the full name of the debug logs
            debug_log_dir: see above
            debug_logger <object>: logging object reference

        public interface:

            updateDebugLog(*args) <void>: accepts variable arguments and both prints to the screen and logs them to a file
            deleteOutdatedDebugLogs() <void>: deletes all debug logs outside a given archive datetime range

        private methods:

            _getDebugLogger(logger_name) <logger>: retrieves or creates a logging object from the given logger_name
    """

    def __init__(self, debug_log_dir, debug_log_basename, debug_log_options):

        self.log_datetime_format = debug_log_options.get('log_datetime_format','%Y-%m-%d')
        self.log_file_extn = debug_log_options.get('log_file_extn','log')
        self.debug_log_archive_days = debug_log_options.get('debug_log_archive_days', 0)

        log_datetime_string = datetime.strftime(datetime.now(), self.log_datetime_format)
        self.debug_log_name =  "%s_%s.%s" % (debug_log_basename, log_datetime_string, self.log_file_extn)
        self.debug_log_dir = debug_log_dir

        if not os.path.isdir(debug_log_dir):
            os.makedirs(debug_log_dir)

        self.debug_logger = self._getDebugLogger(debug_log_basename)

    def _getDebugLogger(self, logger_name):

        txt_handler = RotatingFileHandler(os.path.join(self.debug_log_dir, self.debug_log_name))
        txt_handler.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(txt_handler)

        return logger

    def updateDebugLog(self, *args):

        print args
        self.debug_logger.debug(str(args))

    def deleteOutdatedDebugLogs(self):

        debug_log_archive_days = self.debug_log_archive_days
        current_datetime = datetime.now()
        isOutsideArchiveRange = lambda d:(current_datetime - d).days > int(debug_log_archive_days)
        getDebugLogDatetime = lambda d:datetime.strptime(os.path.basename(d).split("_")[-1].split(".")[0], self.log_datetime_format)
        hasArchiveRangeAndDirectory = debug_log_archive_days > 0 and os.path.isdir(self.debug_log_dir)

        if hasArchiveRangeAndDirectory:
            current_debug_logs = [d for d in os.listdir(self.debug_log_dir) if d.endswith(self.log_file_extn)]
            for debug_log in current_debug_logs:
                if isOutsideArchiveRange(getDebugLogDatetime(debug_log)):
                    os.remove(os.path.join(self.debug_log_dir, debug_log))