
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>
#include <string>
#include <stdexcept>

#include <epicsTime.h>
#include <osiFileName.h>

static const char pvseps_def[] = ":-{}";
const char *pvseps = pvseps_def;

static const char pathsep[] = OSI_PATH_SEPARATOR;

// write the PV name part of the path
std::string pvpathname(const char* pvname)
{
    std::string fname(pvname);
    size_t p = 0, s = fname.size();

    while((p=fname.find_first_of(pvseps, p))!=std::string::npos)
    {
        fname[p] = pathsep[0];
        p++;
        if(p==s)
            break;
    }
    return fname;
}

// Recurisvely create (if needed) the directory components of the path
void createDirs(const std::string& path)
{
    size_t p=0;
    while((p=path.find_first_of(pathsep[0], p))!=std::string::npos)
    {
        std::string part(path.substr(0, p));
        p++;
        if(mkdir(part.c_str(), 0755)!=0)
            switch(errno) {
            case EEXIST:
                break;
            default:
                perror("mkdir");
            }
        else
            std::cerr<<"Create directory "<<part<<"\n";
    }
}

// Get the year in which the given timestamp falls
void getYear(const epicsTimeStamp& t, int *year)
{
    time_t sec = t.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    tm result;
    if(!gmtime_r(&sec, &result))
        throw std::runtime_error("gmtime_r failed");
    *year = 1900 + result.tm_year;
}

// Fetch the first second of the given year
void getStartOfYear(int year, epicsTimeStamp* t)
{
    tm op;
    memset(&op, 0, sizeof(op));
    op.tm_mday = 1;
    op.tm_mon = 0;
    op.tm_year = year - 1900;
    op.tm_yday = 1;
    time_t firstsec = timegm(&op);
    t->secPastEpoch = firstsec - POSIX_TIME_AT_EPICS_EPOCH;
    t->nsec = 0;
}

// Get the year and month in which the given timestamp falls
void getYearMonth(const epicsTimeStamp& t, int *year, int *month)
{
    time_t sec = t.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    tm result;
    if(!gmtime_r(&sec, &result))
        throw std::runtime_error("gmtime_r failed");
    *year = 1900 + result.tm_year;
    *month = 1 + result.tm_mon;
}

// Fetch the first second of the given year, month
void getStartOfYearMonth(int year, int month, epicsTimeStamp* t)
{
#if 0
    if (month>12) {
        // it is assumed that struct tm will be normalized by timegm()
        month = 1;
        year ++;
    }
#endif
    tm op;
    memset(&op, 0, sizeof(op));
    op.tm_mday = 1;
    op.tm_mon = month - 1;
    op.tm_year = year - 1900;
    op.tm_yday = 1;
    time_t firstsec = timegm(&op);
    t->secPastEpoch = firstsec - POSIX_TIME_AT_EPICS_EPOCH;
    t->nsec = 0;
}

int unescape(const char *in, size_t inlen, char *out, size_t outlen)
{
    char *initout = out;
    int escape = 0;

    for(; inlen; inlen--, in++) {
        char I = *in;

        if(escape) {
            escape = 0;
            switch(I) {
            case 1: *out++ = 0x1b; break;
            case 2: *out++ = '\n'; break;
            case 3: *out++ = '\r'; break;
            default:               return 1;
            }
        } else if(I==0x1b){
            escape = 1;
        } else {
            *out++ = I;
        }
    }
    if(initout+outlen!=out)
        return 2;
    return 0;
}

/* compute the size of the unescaped string */
size_t unescape_plan(const char *in, size_t inlen)
{
    size_t outlen = inlen;

    while(inlen-- && outlen>=0) {
        if(*in++ == 0x1b) {
            // skip next
            in++;
            inlen--;
            // remove one from output
            outlen--;
        }
    }

    if(outlen<0)
        return -1;

    return outlen;
}

std::ostream& operator<<(std::ostream& strm, const epicsTime& t)
{
    time_t_wrapper sec(t);
    strm<<ctime(&sec.ts);
    return strm;
}
