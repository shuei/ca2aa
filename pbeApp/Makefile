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
pgsql2pb_LDFLAGS += -L/usr/pgsql-9.5/lib -lpq

PROD_HOST += listpvs
listpvs_SRCS += listpvs.cpp
# Link against XML library that ChannelArchiver links.
listpvs_LDFLAGS += -lxerces-c
#listpvs_LDFLAGS += -lexpat

PROD_HOST += pbexport
pbexport_SRCS += pbexport.cpp
pbexport_SRCS += pbstreams.cpp
pbexport_SRCS += pbeutil.cpp
pbexport_SRCS += EPICSEvent.cpp
# Link against XML library that ChannelArchiver links.
pbexport_LDFLAGS += -lxerces-c
#pbexport_LDFLAGS += -lexpat

TESTPROD_HOST += testPB
testPB_SRCS += testPB.cpp
testPB_SRCS += pbstreams.cpp
testPB_SRCS += pbeutil.cpp
testPB_SRCS += EPICSEvent.cpp
TESTS += testPB

TESTS += testconvert.py

PROD_HOST += pbgentestdata
pbgentestdata_SRCS += genTestData.cpp

PROD_LIBS += Storage Tools ca Com

PROD_SYS_LIBS += protobuf

# override CFLAGS, etc.
USR_CXXFLAGS += -I/usr/pgsql-9.5/include
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

testconvert.py: EPICSEvent_pb2.py
