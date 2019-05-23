TOP=..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

PROD_HOST += pgsql2pb
pgsql2pb_SRCS += pgsql2pb.cpp
pgsql2pb_SRCS += pbstreams.cpp
pgsql2pb_SRCS += pbeutil.cpp
pgsql2pb_SRCS += EPICSEvent.cpp
pgsql2pb_SRCS += PGSQLReader.cpp
pgsql2pb_LDFLAGS += -L${PGSQL_LIBDIR} -lpq

ifdef CHANNELARCHIVER
PROD_HOST += listpvs
listpvs_SRCS += listpvs.cpp
listpvs_LDFLAGS += -l$(LIBXML)

PROD_HOST += pbexport
pbexport_SRCS += pbexport.cpp
pbexport_SRCS += pbstreams.cpp
pbexport_SRCS += pbeutil.cpp
pbexport_SRCS += EPICSEvent.cpp
pbexport_LDFLAGS += -l$(LIBXML)

PROD_HOST += pbgentestdata
pbgentestdata_SRCS += genTestData.cpp

# link against ChannelArchiver libraries
PROD_LIBS += Storage Tools
endif

TESTPROD_HOST += testPB
testPB_SRCS += testPB.cpp
testPB_SRCS += pbstreams.cpp
testPB_SRCS += pbeutil.cpp
testPB_SRCS += EPICSEvent.cpp
TESTS += testPB

TESTS += testconvert.py

PROD_LIBS += ca Com
PROD_SYS_LIBS += protobuf pthread

# override CFLAGS, etc.
USR_CXXFLAGS += -I${PGSQL_INCDIR}
USR_CXXFLAGS += -std=c++0x
OPT_CFLAGS_YES = -O1 -g
OPT_CXXFLAGS_YES = -O1 -g

TESTSCRIPTS_HOST += $(TESTS:%=%.t)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

%.pb.cc %.pb.h: ../%.proto
	protoc -I.. --cpp_out=. $<

%_pb2.py: ../%.proto
	protoc -I.. --python_out=. $<

%.py: ../%.py
	install -m755 $< $@

pgsql2pb$(OBJ): EPICSEvent.pb.h PGSQLReader.h
pbexport$(OBJ): EPICSEvent.pb.h
testPB$(OBJ): EPICSEvent.pb.h
EPICSEvent$(OBJ): EPICSEvent.pb.cc
EPICSEvent.d: EPICSEvent.pb.cc

testconvert.py: EPICSEvent_pb2.py
