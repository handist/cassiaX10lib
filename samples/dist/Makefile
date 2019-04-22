BINARIES=TestColDist TestDistIdMap TestDistCol TestDistMap UnitTestDistIdMap TestDistMapPG TestDistIdMapDistribution TestDistColDistribution TestDistMapDistribution TestDistIdMapBalance
X10CPP=x10c++
X10CPP=/home/tosiyuki/src/x10kobeu/x10.dist/bin/x10c++
#X10OPTS=-sourcepath ../../ -x10rt mpi
X10OPTS=-sourcepath ../../ -x10rt mpi -VERBOSE_CHECKS
MPIRUN=mpirun -np 4

all : $(BINARIES)
#all : TestDistColBalance

clean :
	rm -rf $(BINARIES)

test :
	$(MPIRUN) ./TestDistMap | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistIdMap | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistCol | fgrep "VALIDATE"
	$(MPIRUN) ./UnitTestDistIdMap | fgrep "###"
	$(MPIRUN) ./TestDistMapPG | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistMapDistribution | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistIdMapDistribution | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistColDistribution | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistIdMapBalance | fgrep "VALIDATE"
	$(MPIRUN) ./TestDistColBalance | fgrep "VALIDATE"


TestColDist : TestColDist.x10 WithHistory.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistIdMap : TestDistIdMap.x10 WithHistory.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistIdMapDistribution : TestDistIdMapDistribution.x10 WithHistory.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistIdMapBalance : TestDistIdMapBalance.x10 ../../cassia/dist/DistIdMap.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistCol : TestDistCol.x10 GatherDistCol.x10 GatherDistColLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistColDistribution : TestDistColDistribution.x10 GatherDistCol.x10 GatherDistColLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistColBalance : TestDistColBalance.x10 GatherDistCol.x10 GatherDistColLocal.x10 ../../cassia/dist/DistCol.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistMap : TestDistMap.x10 GatherDistMap.x10 GatherDistMapLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistMapPG : TestDistMapPG.x10 GatherDistMap.x10 GatherDistMapLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistMapDistribution : TestDistMapDistribution.x10 GatherDistMap.x10 GatherDistMapLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestDistMapList : TestDistMapList.x10 GatherDistMapList.x10 GatherDistMapListLocal.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

DebugMap : DebugMap.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

UnitTestDistIdMap : UnitTestDistIdMap.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10

TestTeam : TestTeam.x10
	$(X10CPP) $(X10OPTS) -o $@ $@.x10
