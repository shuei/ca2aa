//////////////////////////////////////////////////////////////////////
// -*- encoding: utf-8 -*-
//
//
//////////////////////////////////////////////////////////////////////

#ifndef PGSQL_READER_H
#define PGSQL_READER_H

// C++
#include <string>
#include <vector>

// PostgreSQL
#include <libpq-fe.h>
#include <pg_config.h>
#if PG_VERSION_NUM < 90200
#error PostgreSQL 9.2 or later is required
#endif

// EPICS base
#include <db_access.h>

// EPICS Channel Archiver
#define DISCONNECTED     3904  // 0x0f40
#define ARCHIVE_OFF      3872  // 0x0f20
#define ARCHIVE_DISABLED 3834  // 0x0f08
#define WRITE_ERROR      3976  // 0x0f88, chosen arbitrary

//////////////////////////////////////////////////////////////////////
class PGSQLReader {
public:
   PGSQLReader(const char *server, const char *database, const char *user, const char *passwd = "", const char *port = 0, const int verbose = 0);
   virtual ~PGSQLReader();

   //
   typedef struct dbr_time_double data_t;
   typedef struct dbr_time_double Data;
   typedef struct {
      int         epicsid;
      int         rdbid;
      std::string rdbstr;
   } alarm_t;

   //
   void                      setVerbose(int v)        { fVerbose = v; }
   data_t                   *find(const std::string &pvname, const int dbr, std::string &start, std::string &end);
   data_t                   *get()                    { return &fSample;}
   data_t                   *next();

   const std::string        &getPVname()        const { return fPVname; }
   int                       getType()          const { return fDBRtype; }
   int                       getCount()         const { return 1; }; // Arrays are not supported

   double                    getDisplayHigh()   const { return fDisplayHigh; }
   double                    getDisplayLow()    const { return fDisplayLow; }
   double                    getHighAlarm()     const { return fHighAlarm; }
   double                    getLowAlarm()      const { return fLowAlarm; }
   double                    getHighWarning()   const { return fHighWarning; }
   double                    getLowWarning()    const { return fLowWarning; }
   int                       getPrecision()     const { return fPrecision; }
   const std::string        &getUnits()         const { return fUnits; }
   int                       getNumStates()     const { return fNumStates; }
   const std::string        &getState(int i)    const { return fState[i]; }

   // helper methods
   static char              *time2str(const time_t sec);
   static time_t             str2time(const char *str);

protected:
   // internal helper methods
   int                       readSeverity();
   int                       readStatus();
   int                       readChannelId();
   int                       readMetadata();
   int                       readEnum();
   int                       getSeverity(const int rdbid);
   int                       getStatus(const int rdbid);
   int                       setStartTime(std::string &timestr);
   int                       setEndTime(std::string &timestr);
   int                       setSingleRowModeQuery();
   int                       readSample();

protected:
   int                       fVerbose;

   PGconn                   *fConn;
   data_t                    fSample;
   std::string               fPVname;
   int                       fChannelId;
   int                       fDBRtype;

   std::vector<alarm_t>      fSeverity;
   std::vector<alarm_t>      fStatus;

   double                    fDisplayHigh;
   double                    fDisplayLow;
   double                    fHighAlarm;
   double                    fLowAlarm;
   double                    fHighWarning;
   double                    fLowWarning;
   int                       fPrecision;
   std::string               fUnits;
   int                       fNumStates;
   std::vector<std::string>  fState;
   double                    fStartTime; // UNIX time
   double                    fEndTime;   // UNIX time
};

#endif
