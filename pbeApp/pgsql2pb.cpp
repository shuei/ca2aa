//////////////////////////////////////////////////////////////////////
// -*- encoding: utf-8 -*-
//
//
//////////////////////////////////////////////////////////////////////

#include <ctime>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <string>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <stdexcept>

#include <unistd.h>

//
#include "PGSQLReader.h"

// Base
#include <epicsVersion.h>
#include <epicsTime.h>
#include <db_access.h>

//
#include "pbsearch.h"
#include "pbstreams.h"
#include "pbeutil.h"
#include "EPICSEvent.pb.h"

// Google Protocol Buffers
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/io/coded_stream.h>

//using namespace std;

struct PBWriter
{
   PGSQLReader& reader;
   // Last returned sample, or NULL if all consumed
   const PGSQLReader::Data *samp;

    // The year and month currently being exported
   int year;
   int month;
   int  dtype;
   bool isarray;
   epicsTimeStamp startofyear;
   //epicsTimeStamp startofboundary;
   epicsTimeStamp endofboundary;

   std::ofstream outpb;
   int typeChangeError;
   const std::string name;
   const std::string outdir;

   PBWriter(PGSQLReader& reader, std::string pv, std::string outdir);
   void write(); // all work is done through this method

   bool prepFile();

   void forwardReaderToTime(unsigned int sampleSec, unsigned int sampleNano);

   void (*skipForward)(PBWriter&,const char *file);

   void (*transcode)(PBWriter&); // Points to a transcode_samples<>() specialization
};


/* Type specific operations helper for transcode_samples<>().
 *  valueop<DBR,isarray>::set(PBClass, dbr_* pointer, # of elements)
 *   Assign a scalar or array to the .val of a PB class instance (ie. EPICS::ScalarDouble)
 */
template<int dbr, int isarray> struct valueop {
   static void set(typename dbrstruct<dbr,isarray>::pbtype& pbc,
                   const typename dbrstruct<dbr,isarray>::dbrtype* pdbr,
                   unsigned)
   {
      pbc.set_val(pdbr->value);
   }
};

// Partial specialization for arrays (works for numerics and scalar string)
// does this work for array of string? Verified by jbobnar: YES, it works for array of strings
template<int dbr> struct valueop<dbr,1> {
   static void set(typename dbrstruct<dbr,1>::pbtype& pbc,
                   const typename dbrstruct<dbr,1>::dbrtype* pdbr,
                   unsigned count)
   {
      pbc.mutable_val()->Reserve(count);
      for(unsigned i=0; i<count; i++)
         pbc.add_val((&pdbr->value)[i]);
   }
};

// specialization for scalar char
template<> struct valueop<DBR_TIME_CHAR,0> {
    static void set(EPICS::ScalarByte& pbc,
                    const dbr_time_char* pdbr,
                    unsigned)
    {
        char buf[2];
        buf[0] = pdbr->value;
        buf[1] = '\0';
        pbc.set_val(buf);
    }
};

// specialization for vector char
template<> struct valueop<DBR_TIME_CHAR,1> {
    static void set(EPICS::VectorChar& pbc,
                    const dbr_time_char* pdbr,
                    unsigned)
    {
        const epicsUInt8 *pbuf = &pdbr->value;
        pbc.set_val((const char*)pbuf);
    }
};

template<int dbr, int isarray>
void transcode_samples(PBWriter& self)
{
   typedef const typename dbrstruct<dbr,isarray>::dbrtype sample_t;
   typedef typename dbrstruct<dbr,isarray>::pbtype encoder_t;
   typedef std::vector<std::pair<std::string, std::string> > fieldvalues_t;


   encoder_t encoder;
   escapingarraystream encbuf;
   fieldvalues_t fieldvalues;

   epicsUInt32 disconnected_epoch = 0;
   int prev_stat = 0;
   unsigned long nwrote=0;
   int last_day_fields_written = 0;

    //prepare the field values and write them every day
    //numeric values have all except PREC, which is only for DOUBLE and FLOAT
    //enum has only labels, string has nothing
    std::stringstream ss;
    if (dbr == DBR_TIME_SHORT || dbr == DBR_TIME_INT || dbr == DBR_TIME_LONG || dbr == DBR_TIME_FLOAT
            || dbr == DBR_TIME_DOUBLE) {
        ss << self.reader.getDisplayHigh();
        fieldvalues.push_back(std::make_pair("HOPR",ss.str()));
        ss.str(""); ss.clear(); ss << self.reader.getDisplayLow();
        fieldvalues.push_back(std::make_pair("LOPR",ss.str()));
        ss.str(""); ss.clear(); ss << self.reader.getUnits();
        fieldvalues.push_back(std::make_pair("EGU",ss.str()));
        if (!isarray) {
            ss.str(""); ss.clear(); ss << self.reader.getHighAlarm();
            fieldvalues.push_back(std::make_pair("HIHI",ss.str()));
            ss.str(""); ss.clear(); ss << self.reader.getHighWarning();
            fieldvalues.push_back(std::make_pair("HIGH",ss.str()));
            ss.str(""); ss.clear(); ss << self.reader.getLowWarning();
            fieldvalues.push_back(std::make_pair("LOW",ss.str()));
            ss.str(""); ss.clear(); ss << self.reader.getLowAlarm();
            fieldvalues.push_back(std::make_pair("LOLO",ss.str()));
        }
    }
    if (dbr == DBR_TIME_FLOAT || dbr == DBR_TIME_DOUBLE) {
        ss.str(""); ss.clear(); ss << self.reader.getPrecision();
        fieldvalues.push_back(std::make_pair("PREC",ss.str()));
    }
    if (dbr == DBR_TIME_ENUM) {
       //if (self.info.getType() == CtrlInfo::Enumerated) {
          size_t i, num = self.reader.getNumStates();
          if (num > 0) {
             std::string state = self.reader.getState(0);
             ss << state.c_str();
             for (i = 1; i < num; i++) {
                state = self.reader.getState(i);
                ss << ";" << state.c_str();
             }
             fieldvalues.push_back(std::make_pair("states",ss.str()));
          }
       //}
    }

    int previousType = self.reader.getType();
    do {
       if (self.reader.getType() != previousType) {
          std::cout << "ERROR: The type of PV " << self.name.c_str() << " changed from " << previousType << " to " << self.reader.getType() << std::endl;
          std::cout << "Wrote: " << nwrote << std::endl;
          self.typeChangeError += 1;
          return;
       }
       previousType = self.reader.getType();
       sample_t *sample = (sample_t*)self.samp;

       if (sample->stamp.secPastEpoch>=self.endofboundary.secPastEpoch) {
          std::cout << "Boundary " << sample->stamp.secPastEpoch << " " << self.endofboundary.secPastEpoch << std::endl;
          std::cout << "Wrote: " << nwrote << std::endl;
          self.typeChangeError = 0;
          return;
       }
       unsigned int secintoyear = sample->stamp.secPastEpoch - self.startofyear.secPastEpoch;

       encoder.Clear();

       int write_fields = 0;
       int day = sample->stamp.secPastEpoch / 86400;
       if (day != last_day_fields_written) {
          //if we switched to a new day, write the fields
          write_fields = 1;
       }

       const dbr_short_t sevr = sample->severity;
       const dbr_short_t stat = sample->status;
       const char *timestr = PGSQLReader::time2str(sample->stamp.secPastEpoch+POSIX_TIME_AT_EPICS_EPOCH);

       // sevr                     ArchiveDataClient.pl
       // stat                     RDB archiver
       // 3904 : Disconnected      "ARCH_DISCONNECT"
       // 3872 : Archive Off       "ARCH_STOPPED"
       // 3848 : Archive Disabled  "ARCH_DISABLED"
       // 3856 : Repeat            "ARCH_REPEAT"
       // 3968 : Est_Repeat        "ARCH_EST_REPEAT"
       // 3976 : Write_Error       CSS Archive specific
       if (stat==3904 || stat==3872 || stat==3848 || stat==3976) {
          if (disconnected_epoch == 0) {
             disconnected_epoch = sample->stamp.secPastEpoch;
          }
          if ((stat==3872 || stat==3848 || stat==3976) && prev_stat < 3000) {
             prev_stat = stat;
          }
          write_fields = 0; //don't write fields if disconnected
          continue;
       } else if (stat >= 3000) {
          //sevr == 3856 || sevr == 3968
          std::cout << "WARN: " << self.name.c_str() << " " << timestr << ": special stat " << stat << " encountered" << std::endl;
          write_fields = 0; //don't write fields if special severity/status
       } else if (stat < 0) {
          // unknown status
          std::cout << "WARN: " << self.name.c_str() << " " << timestr << ": unknown stat " << stat << " encountered" << std::endl;
          //write_fields = 0; //don't write fields if special severity/status
       } else if (sevr < 0) {
          // unknown severity
          std::cout << "WARN: " << self.name.c_str() << " " << timestr << ": unknown sevr " << sevr << " encountered" << std::endl;
          //write_fields = 0; //don't write fields if special severity/status
       } else if (disconnected_epoch != 0) {
          //this is the first sample with value after a disconnected one
          EPICS::FieldValue* FV(encoder.add_fieldvalues());
          std::stringstream str; str << (disconnected_epoch + POSIX_TIME_AT_EPICS_EPOCH);
          FV->set_name("cnxlostepsecs");
          FV->set_val(str.str());

          EPICS::FieldValue* FV2(encoder.add_fieldvalues());
          str.str(""); str.clear(); str << (sample->stamp.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH);
          FV2->set_name("cnxregainedepsecs");
          FV2->set_val(str.str());

          if (prev_stat == 3872) {
             EPICS::FieldValue* FV3(encoder.add_fieldvalues());
             FV3->set_name("startup");
             FV3->set_val("true");
          } else if (prev_stat == 3848) {
             EPICS::FieldValue* FV3(encoder.add_fieldvalues());
             FV3->set_name("resume");
             FV3->set_val("true");
          } else if (prev_stat == 3976) {
             EPICS::FieldValue* FV3(encoder.add_fieldvalues());
             FV3->set_name("writeerror");
             FV3->set_val("true");
          }
          prev_stat = stat;
          disconnected_epoch = 0;
       }

       if (sample->severity!=0)
          encoder.set_severity(sample->severity);
       if (sample->status!=0)
          encoder.set_status(sample->status);

       encoder.set_secondsintoyear(secintoyear);
       encoder.set_nano(sample->stamp.nsec);

       valueop<dbr, isarray>::set(encoder, sample, self.reader.getCount());

       if(fieldvalues.size() && write_fields)
       {
          // encoder accumulated fieldvalues for this sample
          for(fieldvalues_t::const_iterator it=fieldvalues.begin(), end=fieldvalues.end();
              it!=end; ++it)
          {
             EPICS::FieldValue* FV(encoder.add_fieldvalues());
             FV->set_name(it->first);
             FV->set_val(it->second);
          }
          //fieldvalues.clear(); // don't clear the fields, we will use them again later
          last_day_fields_written = day;
       }

       try {
          {
             google::protobuf::io::CodedOutputStream encstrm(&encbuf);
             encoder.SerializeToCodedStream(&encstrm);
          }
          encbuf.finalize();
          self.outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
          nwrote++;
       } catch(std::exception& e) {
          std::cout << "ERROR encoding sample! : " << e.what() << std::endl;
          encbuf.reset();
          // skip
        }

    } while(self.outpb.good() && (self.samp=self.reader.next()));

    std::cout << "End file " << self.samp << " " << self.outpb.good() << std::endl;
    std::cout << "Wrote: " << nwrote << std::endl;
}

void PBWriter::forwardReaderToTime(unsigned int sampleSec, unsigned int sampleNano)
{
   unsigned int sec = sampleSec;
   unsigned int nano = sampleNano;

   //now skip forward to the first sample that is later than the last event read from the file
   unsigned int sampseconds = samp->stamp.secPastEpoch;
   while ((sampseconds < sec) && samp) {
      samp = reader.next();
      if (samp)
         sampseconds = samp->stamp.secPastEpoch;
   }

   if (samp && (sampseconds == sec)) {
      unsigned int sampnano = samp->stamp.nsec; //in some cases I got overflow!?
      while (samp && (sampseconds == sec && sampnano <= nano)) {
         samp = reader.next();
         if (samp) {
            sampseconds = samp->stamp.secPastEpoch;
            sampnano = samp->stamp.nsec;
         }
      }
   }
}

template<int dbr, int array>
void skip(PBWriter& self, const char* file)
{
#if 0
   typedef typename dbrstruct<dbr, array>::pbtype decoder;

   decoder sample;
   sample = searcher<dbr,array>::getLastSample(file);

   // There is a room for optimization:
   // 1) Cancel current query
   // 2) Extract Start-of-query time from sample
   // 3) Issue a new query
   std::cout << "skip not implemented yet" << std::endl;
   exit(-1);
#else
   typedef typename dbrstruct<dbr, array>::pbtype decoder;

   decoder sample;
   sample = searcher<dbr,array>::getLastSample(file);
   std::cout << "Skipping until " << PGSQLReader::time2str(sample.secondsintoyear() + self.startofyear.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH) << std::endl;
   self.forwardReaderToTime(sample.secondsintoyear() + self.startofyear.secPastEpoch, sample.nano());
#endif
}

bool PBWriter::prepFile()
{
   const PGSQLReader::Data *samp(reader.get());
//    typedef const typename dbrstruct<dbr,isarray>::dbrtype sample_t;
//    const dbrstruct<DBR_TIME_SHORT,0>::dbrtype *samp((const dbrstruct<DBR_TIME_SHORT,0>::dbrtype*)reader.get()); // this is OK - shuei
   getYearMonth(samp->stamp, &year, &month);

   //boundary b = PARTITION_YEAR;
   boundary b = PARTITION_MONTH;
   switch (b) {
   case PARTITION_YEAR:
      getStartOfYear(year, &startofyear);
      //getStartOfYear(year, &startofboundary);
      getStartOfYear(year+1, &endofboundary);
      break;
   case PARTITION_MONTH:
      getStartOfYear(year, &startofyear);
      //getStartOfYearMonth(year, month, &startofboundary);
      getStartOfYearMonth(year, month+1, &endofboundary);
      break;
   default:
      std::ostringstream msg;
      msg << "Unsupported Partition " << b;
      throw std::runtime_error(msg.str());
   }

//    std::cout << "startofyear     : " << startofyear;
//    //std::cout << "startofboundary : " << startofboundary;
//    std::cout << "endofboundary   : " << endofboundary;

   dtype = reader.getType();
   isarray = reader.getCount()!=1;

   EPICS::PayloadInfo header;

   std::cout << "is a " << (isarray?"array":"scalar") << std::endl;
   //exit(-1);

   if (!isarray) {
      // Scalars
      switch(dtype)
      {
#define CASE(DBR) case DBR: transcode = &transcode_samples<DBR, 0>; \
         skipForward = &skip<DBR, 0>;                                   \
    header.set_type((EPICS::PayloadType)dbrstruct<DBR, 0>::pbcode); break
         CASE(DBR_TIME_STRING);
         CASE(DBR_TIME_CHAR);
         CASE(DBR_TIME_SHORT);
         CASE(DBR_TIME_ENUM);
         CASE(DBR_TIME_LONG);
         CASE(DBR_TIME_FLOAT);
         CASE(DBR_TIME_DOUBLE);
#undef CASE
      default: {
         std::ostringstream msg;
         msg << "Unsupported type " << dtype;
         throw std::runtime_error(msg.str());
      }
      }
   } else {
      // Vectors
      switch(dtype)
      {
#define CASE(DBR) case DBR: transcode = &transcode_samples<DBR, 1>; \
         skipForward = &skip<DBR, 1>;                                   \
         header.set_type((EPICS::PayloadType)dbrstruct<DBR, 1>::pbcode); break
         CASE(DBR_TIME_STRING);
         CASE(DBR_TIME_CHAR);
         CASE(DBR_TIME_SHORT);
         CASE(DBR_TIME_ENUM);
         CASE(DBR_TIME_LONG);
         CASE(DBR_TIME_FLOAT);
         CASE(DBR_TIME_DOUBLE);
#undef CASE
      default: {
         std::ostringstream msg;
         msg << "Unsupported type " << dtype;
         throw std::runtime_error(msg.str());
      }
      }
   }

   header.set_elementcount(reader.getCount());
   header.set_year(year);
   header.set_pvname(reader.getPVname());

   std::ostringstream fname;
   switch (b) {
   case PARTITION_YEAR:
      fname << outdir << pvpathname(reader.getPVname().c_str()) << ":" << year << ".pb";
      break;
   case PARTITION_MONTH:
      fname << outdir << pvpathname(reader.getPVname().c_str()) << ":" << year << "_" << std::setfill('0') << std::setw(2) << std::right << month << ".pb";
      break;
   default:
      std::ostringstream msg;
      msg << "Unsupported Partition " << b;
      throw std::runtime_error(msg.str());
   }
   if (typeChangeError > 0) {
      fname << "." << typeChangeError;
   }

   int fileexists = 0;
   {
      FILE *fp = fopen(fname.str().c_str(), "r");
      if(fp) {
         fclose(fp);
         fileexists = 1;
         try {
            (*skipForward)(*this,fname.str().c_str());
         } catch (std::invalid_argument& e) {
            //invalid argument is thrown when the sample type
            //doesn't match the type in the existing file
            typeChangeError++;
            return false;
         }
      }
   }

   std::cout << "Starting to write " << fname.str() << std::endl;
   createDirs(fname.str());

   escapingarraystream encbuf;
   {
      if (!fileexists) {
         google::protobuf::io::CodedOutputStream encstrm(&encbuf);
         header.SerializeToCodedStream(&encstrm);
      }
   }
   encbuf.finalize();

   outpb.open(fname.str().c_str(), std::fstream::app);
   if (!fileexists) { //if file exists do not write header
      outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
   }
   return true;
}

PBWriter::PBWriter(PGSQLReader& reader, std::string pv, std::string outdir)
:reader(reader)
,year(0)
,name(pv)
,outdir(outdir)
{
   samp = reader.get();
}

void PBWriter::write()
{
   typeChangeError = 0;
   while(samp) {
      try {
         if (prepFile()) {
            if (!samp) {
               std::cout << __FILE__ << " " << __func__ << " " << samp << std::endl;
               break;
            }
            (*transcode)(*this);
         }
#if 0
      } catch (GenericException& up) {
         if (std::strstr(up.what(),"Error in data header")) {
            // From RawDataReader::getHeader()
            //Error in the data header means a corrupted sample data.
            //It can happen in the prepFile or in the transcode. Either way the resolution is the same.
            //We try to move ahead. If it doesn't work, abort.
            std::cout << "ERROR: " << name.c_str() << ": Corrupted header, continuing with the next sample." << std::endl << up.what() << std::endl;
            samp = reader.next();
         } else {
            //tough luck
            outpb.close();
            throw;
         }
#endif
      } catch(...) {
         outpb.close();
         throw;
      }

      bool ok = outpb.good();
      outpb.close();
      if (!ok) {
         std::cout << "Error writing file" << std::endl;
         break;
      }
   }
}

// supported DBR types
struct DBRtype_t { int num; std::string str; };
#define TYPE(t) {t, #t}
static std::vector<DBRtype_t> kDBRtypes = {
   TYPE(DBR_TIME_ENUM),
   TYPE(DBR_TIME_DOUBLE),
};

void usage(const char *argv0)
{
   const char *pv    = "SOME:PVNAME";
   const char *start = "2017-02-01T00:00:00";
   const char *end   = "2017-02-02T00:00:00";
   const char *type  = "DBR_TIME_DOUBLE";

   std::cout << "Usage: " << argv0 << "[-h] [-v] [-o OUTDIR] [-s START] [-e END] -t DBRTYPE PV [PV ...]" << std::endl
             << std::endl
             << "Example: " << std::endl
             << argv0 << " -s " << start << " -e " << end << " -t " << type << " " << pv
             << std::endl
             << "Options:" << std::endl
             << " -h         : Print this message." << std::endl
             << " -v         : Increase verbosity." << std::endl
             << " -t DBRTYPE : Specify DBR_TIME_xxxx (required)" << std::endl
             << "              Both string expression (e.g. DBR_TIME_ENUM)" << std::endl
             << "              and numeric expression (e.g. 20) are accepted." << std::endl
             << "              Supported types are:" << std::endl;
   for (auto itr = kDBRtypes.begin(); itr!=kDBRtypes.end(); ++itr) {
      std::cout << "              " << itr->str << std::endl;
   }
   std::cout << " -o OUTDIR  : Specify output directory." << std::endl
             << " -s START   : Start of query window." << std::endl
             << " -e END     : End of query winrow." << std::endl
             << "              Acceptable date formtas are:" << std::endl
             << "              YYYYMMDDThhmmss" << std::endl
             << "              YYYYMMDD hhmmss" << std::endl
             << std::endl;

   exit(EXIT_FAILURE);
}

int str2dbrtype(const std::string &str)
{
   // Numeric expression
   if (std::all_of(str.begin(), str.end(), ::isdigit)) {
      return stoi(str);
   }

   // String expression
   for (auto itr = kDBRtypes.begin(); itr!=kDBRtypes.end(); ++itr) {
      // std::cout << itr->num << " " << itr->str <<std::endl;
      if (str == itr->str) {
         return itr->num;
      }
   }

   // unsupported DBRTYPE
   return -1;
}

int main(int argc, char *argv[])
{
    //comment this if you want to see the protobuf logs
   google::protobuf::LogSilencer *silencer = new google::protobuf::LogSilencer();

   const  char *argv0 = argv[0];

   //
   int          dbrtype = -1;
   std::string  outdir("./");
   std::string  start = "";
   std::string  end   = "";
   int          verbose = 0;

   // parse command line options
   int ch;
   extern char *optarg;
   extern int   optind;
   while ((ch=getopt(argc, argv, "ho:s:e:t:v")) != EOF) {
      //char *endp;
      switch(ch) {
      case 'h':
         usage(argv0);
         break;
      case 'v':
         verbose ++;
         break;
      case 't':
         dbrtype = str2dbrtype(optarg);
         break;
      case 's':
         start = optarg;
         break;
      case 'e':
         end = optarg;
         break;
      case 'o':
         outdir = optarg;
         if (outdir[outdir.size()-1] != '/') {
            outdir.push_back('/');
         }
         break;
      default:
         usage(argv0);
         break;
      }
   }

   argc -= optind;
   argv += optind;

   if (dbrtype<0) {
      std::cout << "-t : DBRTYPE missing or unsupported" << std::endl;
      usage(argv0);
   }

   if (argc<=0) {
      usage(argv0);
   }

   //
   try {
      {
         char *seps = getenv("NAMESEPS");
         if(seps) {
            pvseps = seps;
         }
      }

      const char *server = "your.postgresql.server";
      const char *dbname = "archive";
      const char *user   = "report";
      const char *passwd = "";
      const char *port   = 0;

      PGSQLReader *reader = new PGSQLReader(server, dbname, user, passwd, port, verbose);

      for (int i=0; i<argc; i++) {

         const char *pvname = argv[i];
         //std::string pvname(*argv);

         try {
            std::cout << "Visit PV " << pvname << std::endl;

            if(!reader->find(pvname, dbrtype, start, end)) {
               // PV not found or no data in the query window
               continue;
            }

            std::cout << " start " << start << " end " << end << std::endl;
            std::cout << " Type " << reader->getType() << " count " << reader->getCount() << std::endl;

            PBWriter writer(*reader, pvname, outdir);
            writer.write();
         } catch (std::exception& e) {
            //print exception and continue with the next pv
            std::cout << "Exception: " << pvname << ": " << e.what() << std::endl;
         }
         std::cout << "Done" << std::endl;
      }

      std::cout << "Done" << std::endl;
      delete silencer;
      return EXIT_SUCCESS;
   } catch(std::exception& e ){
      std::cout << "Exception: " << e.what() << std::endl;
      return EXIT_FAILURE;
   }
}
