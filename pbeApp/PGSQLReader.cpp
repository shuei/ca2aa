//////////////////////////////////////////////////////////////////////
// -*- encoding: utf-8 -*-
//
//
//////////////////////////////////////////////////////////////////////

// C++
#include <algorithm>
#include <iostream>
#include <sstream>

// PostgreSQL
#include <libpq-fe.h>

// EPICS base
#include <alarm.h>
#include <alarmString.h>
#include <epicsTime.h>

// PostgreSQL
#include "PGSQLReader.h"

//////////////////////////////////////////////////////////////////////
//
// static methods
//
static const char *timefmt0 = "%Y-%m-%dT%H:%M:%S";
static const char *timefmt1 = "%Y-%m-%d %H:%M:%S";
char *PGSQLReader::time2str(const time_t sec)
{
   struct  tm     *tm = localtime(&sec);
   const   size_t  len = 128;
   static  char    buf[len];
   strftime(buf, len, timefmt0, tm);
   //printf("%s\n", buf);
   return buf;
}

time_t PGSQLReader::str2time(const char *str)
{
   struct tm tm;
   char  *p;

   p = strptime(str, timefmt0, &tm);
   //printf("[%ld]\n", p);
   if (!p) {
      p = strptime(str, timefmt1, &tm);
      //printf("[%ld]\n", p);
   }

   if (!p) {
      printf("ERROR parsing time: %s\n", str);
      exit(-1);
   }

   return timelocal(&tm);
}

//////////////////////////////////////////////////////////////////////
//
// Ctor
//
PGSQLReader::PGSQLReader(const char *server, const char *dbname, const char *user, const char *passwd, const char *port, const int verbose)
:fVerbose(verbose)
,fSample()
,fPVname("")
,fChannelId(0)
,fDBRtype(-1)
,fSeverity(ALARM_NSEV)
,fStatus(ALARM_NSTATUS)
,fDisplayHigh(0)
,fDisplayLow(0)
,fHighAlarm(0)
,fLowAlarm(0)
,fHighWarning(0)
,fLowWarning(0)
,fPrecision(0)
,fUnits("")
,fNumStates(0)
,fState(0)
{
   fConn = PQsetdbLogin(server, port, NULL, NULL, dbname, user, passwd);
   if (PQstatus(fConn) == CONNECTION_BAD) {
      printf("ERROR: %s\n", PQerrorMessage(fConn));
      exit(-1); // we'd beeter throw exception
   }
   tzset();

   readSeverity();
   readStatus();
}

//////////////////////////////////////////////////////////////////////
//
// Dtor
//
PGSQLReader::~PGSQLReader()
{
   if (fConn) {
      PQfinish(fConn);
   }
}

//////////////////////////////////////////////////////////////////////
//
// Find and read the first sample in the time window
//
PGSQLReader::data_t *PGSQLReader::find(const std::string &pvname, const int dbr, std::string &start, std::string &end)
{
   fPVname = pvname;
   fDBRtype = dbr;

   if (readChannelId()<=0) {
      printf("ERROR: PV not found: %s\n", fPVname.c_str());
      return 0;
   }

   readMetadata();
   readEnum();

   setStartTime(start);
   setEndTime(end);

   setSingleRowModeQuery();

   if (fVerbose>0) printf("#####\n#%s\n", __func__);

   if (readSample()) {
      return &fSample;
   }

   printf("Warning: no data in the query window: %s [%s %s]\n", fPVname.c_str(), start.c_str(), end.c_str());
   return 0;
}

//////////////////////////////////////////////////////////////////////
//
// Read the next sample
//
PGSQLReader::data_t *PGSQLReader::next()
{
   if (readSample()) {
      return &fSample;
   }

   return 0;
}

//////////////////////////////////////////////////////////////////////
//
// map severity_id in the RDB to severity in EPICS base
//
int PGSQLReader::getSeverity(int rdbid)
{
   const int n = fSeverity.size();
   for(int i=0; i<n; i++) {
      if (fSeverity[i].rdbid == rdbid) {
         return fSeverity[i].epicsid;
      }
   }
   return -(n+1);
}

//////////////////////////////////////////////////////////////////////
//
// map status_id in the RDB to alarm condition in EPICS base
//
int PGSQLReader::getStatus(int rdbid)
{
   const int n = fStatus.size();
   for(int i=0; i<n; i++) {
      if (fStatus[i].rdbid == rdbid) {
         return fStatus[i].epicsid;
      }
   }
   return -(n+1);
}

//////////////////////////////////////////////////////////////////////
//
// Extract channel_id
//
int PGSQLReader::readChannelId()
{
   std::ostringstream query;
   query << "SELECT channel_id FROM channel WHERE name='" << fPVname << "'";
   PGresult *resp = PQexec(fConn, query.str().c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   // clear
   fChannelId = 0;

   // Query results
   const int nrow = PQntuples(resp);
   if (nrow == 1) {
      char *str = PQgetvalue(resp, 0, 0);
      if (sscanf(str, " %d ", &fChannelId) == 1) {
         if (fVerbose>0) printf("#####\n# %s %d\n", fPVname.c_str(), fChannelId);
      } else {
         // this may not happen.
         printf("ERROR: PV found but no channel_id: %s\n", fPVname.c_str());
         exit(-1);
      }
   } else if (nrow==0) {
      // Return 0 silently when specified PV not found.
      // printf("ERROR: PV not found: %s\n", fPVname.c_str());
   } else if (nrow>1) {
      // this may not happen
      printf("ERROR: found multiple ID for PV: %s\n", fPVname.c_str());
      exit(-1);
   }

   // Cleau up
   PQclear(resp);
   return fChannelId;
}

//////////////////////////////////////////////////////////////////////
//
// Extract severities from RDB
//
int PGSQLReader::readSeverity()
{
   std::string query = "SELECT severity_id, name FROM severity ORDER by severity_id";

   PGresult *resp = PQexec(fConn, query.c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   // Query results
   const unsigned nrow = PQntuples(resp);
   if (nrow>fSeverity.size()) {
      fSeverity.resize(nrow); //
   }

   if (fVerbose>0) printf("#####\n# severity\n");
   for(unsigned i=0; i<nrow; i++) {
      int         &epicsid = fSeverity[i].epicsid;
      int         &rdbid   = fSeverity[i].rdbid;
      std::string &rdbstr  = fSeverity[i].rdbstr;
      sscanf(PQgetvalue(resp, i, 0), " %d ", &rdbid);
      rdbstr = PQgetvalue(resp, i, 1);
      transform (rdbstr.begin(), rdbstr.end(), rdbstr.begin (), ::toupper);
      epicsid = -i;

      // Map severity_id in RDB to those of EPICS base.
      if (rdbstr == "OK" || rdbstr == "NONE") {
         // special treatment for "OK" and "NONE"
         epicsid = NO_ALARM;
         rdbstr = epicsAlarmSeverityStrings[NO_ALARM];
      } else {
         for (unsigned j=1; j<ALARM_NSEV; j++) {
            if (rdbstr == epicsAlarmSeverityStrings[j]) {
               epicsid = j;
            }
         }
      }

      if (fVerbose>0) printf("# rdb:%4d epics:%4d %s\n" , fSeverity[i].rdbid, fSeverity[i].epicsid, fSeverity[i].rdbstr.c_str());
   }

   // Clean-up
   PQclear(resp);
   return 1;
}

//////////////////////////////////////////////////////////////////////
//
// Extract Alarm conditions from RDB
//
int PGSQLReader::readStatus()
{
   std::string query = "SELECT status_id, name FROM status ORDER by status_id";

   PGresult *resp = PQexec(fConn, query.c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   // Query result
   const unsigned nrow = PQntuples(resp);
   if (nrow>fStatus.size()) {
      fStatus.resize(nrow); //
   }

   if (fVerbose>0) printf("#####\n# status\n");
   for(unsigned i=0; i<nrow; i++) {
      int         &epicsid = fStatus[i].epicsid;
      int         &rdbid   = fStatus[i].rdbid;
      std::string &rdbstr  = fStatus[i].rdbstr;
      sscanf(PQgetvalue(resp, i, 0), " %d ", &rdbid);
      rdbstr = PQgetvalue(resp, i, 1);
      transform (rdbstr.begin(), rdbstr.end(), rdbstr.begin (), ::toupper);
      epicsid = -i;

      // Map status_id in RDB to those of EPICS base.
      // Special values are used for disconnected, archive_off, and
      // archive_disabled. Note that in Channel Archiver these items
      // are expressed using severity.
      if (rdbstr == "OK" || rdbstr == "NO_ALARM") { // special treatment for "OK" and "NONE"
         epicsid = NO_ALARM;
         rdbstr = epicsAlarmConditionStrings[NO_ALARM];
      } else if (rdbstr == "DISCONNECTED") {
         epicsid = DISCONNECTED;
      } else if (rdbstr == "ARCHIVE_OFF") {
         epicsid = ARCHIVE_OFF;
      } else if (rdbstr == "ARCHIVE_DISABLED") {
         epicsid = ARCHIVE_DISABLED;
      } else if (rdbstr == "WRITE_ERROR") {
         // WRITE_ERROR does not exist in Channel Archiver.
         // It is specific to CSS Archiver but neither num_val nor float_val are recorded, so we assign a value.
         epicsid = WRITE_ERROR;
      } else {
         for (unsigned j=1; j<ALARM_NSTATUS; j++) {
            std::string tmp(epicsAlarmConditionStrings[j]);
            tmp += "_ALARM";
            if (rdbstr == tmp) {
               rdbstr = std::string(epicsAlarmConditionStrings[j]);
               epicsid = j;
            }
         }
      }

      if (fVerbose>0) printf("# rdb:%4d epics:%4d %s\n" , fStatus[i].rdbid, fStatus[i].epicsid, fStatus[i].rdbstr.c_str());
   }

   // Clean-up
   PQclear(resp);
   return 1;
}

//////////////////////////////////////////////////////////////////////
//
// Extract metadata of the PV, if any.
//
int PGSQLReader::readMetadata()
{
   std::ostringstream query;
   query << " SELECT channel_id, low_disp_rng, high_disp_rng, low_warn_lmt, high_warn_lmt, low_alarm_lmt, high_alarm_lmt, prec, unit"
         << " FROM num_metadata"
         << " WHERE channel_id=" << fChannelId
         ;

   PGresult *resp = PQexec(fConn, query.str().c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   //
   int retval = 0;

   // Query result
   const int nrow = PQntuples(resp);
   if (nrow==1) {
      retval = 1;
      sscanf(PQgetvalue(resp, 0, 1), " %lf ", &fDisplayLow);
      sscanf(PQgetvalue(resp, 0, 2), " %lf ", &fDisplayHigh);
      sscanf(PQgetvalue(resp, 0, 3), " %lf ", &fLowWarning);
      sscanf(PQgetvalue(resp, 0, 4), " %lf ", &fHighWarning);
      sscanf(PQgetvalue(resp, 0, 5), " %lf ", &fLowAlarm);
      sscanf(PQgetvalue(resp, 0, 6), " %lf ", &fHighAlarm);
      sscanf(PQgetvalue(resp, 0, 7), " %d ",  &fPrecision);
      fUnits = PQgetvalue(resp, 0, 8);

      if (fVerbose>0) {
         printf("#####\n# metadata\n#");
         printf(" [%+11.3le:%+11.3le] [%+11.3le:%+11.3le] [%+11.3le:%+11.3le] %3d [%s]\n"
                , fDisplayLow
                , fDisplayHigh
                , fLowWarning
                , fHighWarning
                , fLowAlarm
                , fHighAlarm
                , fPrecision
                , fUnits.c_str()
                );
      }
   } else if (nrow==0) {
      // no metadata
      retval = 1;
      //printf("#####\n# metadata not found\n");
   } else {
      // this shall not happen
      printf("ERROR: found multiple metadata for ID: %d\n", fChannelId);
      exit(-1);
   }

   // Clean-up
   PQclear(resp);
   return retval;
}

//////////////////////////////////////////////////////////////////////
//
// Extract ENUM labels of the PV, if any.
//
int PGSQLReader::readEnum()
{
   std::ostringstream query;
   query << " SELECT channel_id, enum_nbr, enum_val"
         << " FROM enum_metadata"
         << " WHERE channel_id=" << fChannelId
         << " ORDER BY enum_nbr"
         ;
   PGresult *resp = PQexec(fConn, query.str().c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   fNumStates = 0;

   // Query results
   const int nrow = PQntuples(resp);
   if (nrow==0) {
      // printf("#####\n# enum states not found\n");
   } else {
      // get maximum
      sscanf(PQgetvalue(resp, nrow-1, 1), " %d ", &fNumStates);
      fNumStates ++;
      fState.resize(fNumStates);

      if (fVerbose>0) printf("#####\n# enum\n#");
      for (int i=0; i<nrow; i++) {
         size_t      nbr = 0;
         sscanf(PQgetvalue(resp, i, 1), " %zd ", &nbr);
         std::string str = PQgetvalue(resp, i, 2);
         fState[nbr] = str;
         if (fVerbose>0) {
            printf(" %2zd: [%s]"
                   , nbr
                   , str.c_str()
                   );
         }
      }
      if (fVerbose>0) printf("\n");
   }

   // Clean-up
   PQclear(resp);
   return fNumStates;
}

//////////////////////////////////////////////////////////////////////
//
// set start time of the query, otherwise read timestamp (in localtime) of the first sample
//
int PGSQLReader::setStartTime(std::string &timestr)
{
   if (timestr.size()>0) {
      // Normalize given time
      fStartTime = str2time(timestr.c_str());
      timestr = time2str(fStartTime);
      if (fVerbose>0) printf("#####\n#start %s\n", timestr.c_str());
      return 1;
   }

   std::ostringstream query;
   query
         << " SELECT smpl_time"
         << " FROM sample"
         << " WHERE channel_id=" << fChannelId
         << " ORDER BY smpl_time LIMIT 1"
         ;

   PGresult *resp = PQexec(fConn, query.str().c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   fStartTime = -1;

   // Query result
   const int nrow = PQntuples(resp);
   if (nrow==1) {
      //printf("%s %s\n", __func__, PQgetvalue(resp, 0, 0));
      char *s = PQgetvalue(resp, 0, 0);
      fStartTime = str2time(s);
      timestr = time2str(fStartTime);
      if (fVerbose>0) printf("#####\n#start %s\n", timestr.c_str());
   }

   // Clean-up
   PQclear(resp);
   return nrow;
}

//////////////////////////////////////////////////////////////////////
//
// set end time of the query, otherwise read timestamp (in localtime) of the last sample
//
int PGSQLReader::setEndTime(std::string &timestr)
{
   if (timestr.size()>0) {
      // Normalize given time
      fEndTime = str2time(timestr.c_str());
      fEndTime += 1 ; // add extra 1 second for end of the time window
      timestr = time2str(fEndTime);
      if (fVerbose>0) printf("#####\n#end %s\n", timestr.c_str());
      return 1;
   }

   std::ostringstream query;
   query
         << " SELECT smpl_time"
         << " FROM sample"
         << " WHERE channel_id=" << fChannelId
         << " ORDER BY smpl_time DESC LIMIT 1"
         ;

   PGresult *resp = PQexec(fConn, query.str().c_str());

   // Error check
   if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }

   fEndTime = -1;

   // Query result
   const int nrow = PQntuples(resp);
   if (nrow==1) {
      //printf("%s %s\n", __func__, PQgetvalue(resp, 0, 0));
      char *s = PQgetvalue(resp, 0, 0);
      fEndTime = str2time(s);
      fEndTime += 1 ; // add extra 1 second for end of the time window
      timestr = time2str(fEndTime);
      if (fVerbose>0) printf("#####\n#end %s\n", timestr.c_str());
   }

   // Clean-up
   PQclear(resp);
   return nrow;
}

//////////////////////////////////////////////////////////////////////
//
// Query samples in row-by-row mode
//
int PGSQLReader::setSingleRowModeQuery()
{
   std::string start = time2str(fStartTime);
   std::string end   = time2str(fEndTime);
   std::ostringstream query;
   query
         << " SELECT smpl_time, nanosecs, severity_id, status_id, num_val, float_val, str_val, datatype, array_val"
         << " FROM sample"
         << " WHERE channel_id=" << fChannelId
         << " AND smpl_time >= '" << start << "' AND smpl_time <= '" << end << "'"
         << " ORDER BY smpl_time"
         ;

   const int ret = PQsendQuery(fConn, query.str().c_str());
   if (ret==0) {
      printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
      exit(-1);
   }
   PQsetSingleRowMode(fConn);

   return 1;
}

//////////////////////////////////////////////////////////////////////
//
// Read single sample from RDB and fill into dbr_time_xxx
//
int PGSQLReader::readSample()
{
   PGresult *resp = PQgetResult(fConn);
   if (resp == NULL) {
      // Query in row-by-row mode was successfully finished
      // Just for sure - we may not reach here.
      return 0;
   }

   // Query result
   if (PQresultStatus(resp) == PGRES_SINGLE_TUPLE) {
      // fill data into dbr_time_xxx object
      char *s = PQgetvalue(resp, 0, 0);
      double timestamp = str2time(s);         // fractional part will be lost
      timestamp -= POSIX_TIME_AT_EPICS_EPOCH; // conversion from GMT to epics time
      int nanosecs = 0;
      sscanf(PQgetvalue(resp, 0, 1), " %d ", &nanosecs);
      timestamp += nanosecs * 1e-9;           // compensate for the fractional part

      int severity_id = 0;
      sscanf(PQgetvalue(resp, 0, 2), " %d ", &severity_id);
      int status_id = 0;
      sscanf(PQgetvalue(resp, 0, 3), " %d ", &status_id);

      fSample.stamp.secPastEpoch = timestamp;
      fSample.stamp.nsec = nanosecs;
      fSample.status = getStatus(status_id);
      fSample.severity = getSeverity(severity_id);

      if (fVerbose>1) printf("%s (%09d) %4d[%4d] %4d[%4d] [%d:%s] [%d:%s]"
                             , PQgetvalue(resp, 0, 0), nanosecs
                             , fSample.severity, severity_id
                             , fSample.status, status_id
                             , !PQgetisnull(resp, 0, 4)
                             , PQgetvalue(resp, 0, 4)
                             , !PQgetisnull(resp, 0, 5)
                             , PQgetvalue(resp, 0, 5)
             );

      reinterpret_cast<dbr_time_enum *>(&fSample)->value = 0xdead;
      reinterpret_cast<dbr_time_long *>(&fSample)->value = 0xdeadbeef;
      reinterpret_cast<dbr_time_double *>(&fSample)->value = 0x7ff7dead7fb7beef;

      if (fSample.severity==INVALID_ALARM && fSample.status>=3000) {
         // special treatment for Archiver specific status
         if (fVerbose>1) printf(" <duymmy data: 0x%08x>", reinterpret_cast<dbr_time_long *>(&fSample)->value);
      } else if (!PQgetisnull(resp, 0, 4) && PQgetisnull(resp, 0, 5)) {
         // num_val is not empty, float_val is empty
         int num_val = 0;
         if (sscanf(PQgetvalue(resp, 0, 4), " %d ", &num_val) == 1) {
            if (fVerbose>1) printf(" %10d", num_val);
            switch(fDBRtype) {
            case DBR_TIME_DOUBLE:
               fSample.value = num_val;
               break;
            case DBR_TIME_ENUM:
               reinterpret_cast<dbr_time_enum*>(&fSample)->value = num_val;
               break;
            default:
               printf("ERROR: Unsupported DBRTYPE: %d\n", fDBRtype);
               exit(-1);
            }
         } else {
            // this may not happen.
            printf("ERROR: PQgetvalue(resp, 0, 4) = \"%s\"\n", PQgetvalue(resp, 0, 4));
            exit(-1);
         }
      } else if (PQgetisnull(resp, 0, 4) && !PQgetisnull(resp, 0, 5)) {
         // num_val is empty, float_val is not empty
         double float_val = 0;
         if (sscanf(PQgetvalue(resp, 0, 5), " %lf ", &float_val) == 1) {
            if (fVerbose>1) printf(" %.10lf", float_val);
            switch(fDBRtype) {
            case DBR_TIME_DOUBLE:
               // something is wrong
               //reinterpret_cast<dbr_time_double*>(&fSample)->value = float_val;
               fSample.value = float_val;
               break;
#if 0
            case DBR_TIME_ENUM:
               (*dbr_time_enum)(&fSample)->value = float_val;
               break;
#endif
            default:
               printf("ERROR: Unsupported DBRTYPE: %d\n", fDBRtype);
               exit(-1);
            }
         } else {
            // this may not happen.
            printf("ERROR: PQgetvalue(resp, 0, 5) = \"%s\"\n", PQgetvalue(resp, 0, 5));
         }
      } else {
         // this may not happen - something is wrong.
         printf("<N/A>\n");
         exit(-1);
      }
      if (fVerbose>1) printf("\n");

      // Clean-up
      PQclear(resp);
      return 1;

   } else {
      // Error check
      if (PQresultStatus(resp) != PGRES_TUPLES_OK) {
         printf("%s: %d: ERROR: %s\n", __func__, __LINE__, PQerrorMessage(fConn));
         exit(-1);
      }

      // Clean-up
      PQclear(resp);

      // If the query returns zero rows in row-by-row mode, status PGRES_TUPLES_OK will be returned.
      // It is necesarry to continue calling PQgetResult() until it returns NULL.
      resp = PQgetResult(fConn);
      if (resp == NULL) {
         // Query in row-by-row mode was successfully finished
         return 0;
      }
   }

   // we may not reach here
   return 0;
}

//////////////////////////////////////////////////////////////////////
// end
//////////////////////////////////////////////////////////////////////
