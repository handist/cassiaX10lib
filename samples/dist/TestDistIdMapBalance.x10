package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestDistIdMapBalance {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
//    val home = PlaceGroup(0): PlaceGroup;
    val team: Team;

    val numData: Long = 200;

    val distIdMap: DistIdMap[String];
    val random: Random;

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
        this.random = new Random(12345);
	distIdMap = new DistIdMap[String](placeGroup, team);
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestDistIdMapBalance(pg, team).run();
    }


    public def genRandStr(header: String): String {
        val rand = random.nextLong();
	return header + rand.toString();
    }

    public def run(): void {

        // Create initial data at Place 0
        Console.OUT.println("### Create initial data at Place 0");

	for (i in 0..(numData - 1)) {
	    distIdMap(i) = genRandStr("v");
	}

	val gather = new GatherDistIdMap[String](placeGroup, distIdMap);
	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

	// ---------------------------------------------------------------------------

        // Distribute all entries
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Distribute all entries");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distIdMap.each((key: Long, value: String) => {
		val d = key % NPLACES;
		Console.OUT.println("" + here + " moves key: " + key + " to " + d);
		distIdMap.moveAtSync(key, placeGroup(d), mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = key % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 1-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 1-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Distribute all entries");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 1-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 1-2: FAIL");
	}


        // ReDistribute all entries using balance() Pattern 1
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 1");
	placeGroup.broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
            val locality = new Rail[Float](placeGroup.size());
            for (i in 0..(placeGroup.size() - 1)) {
                locality(i) = (i + (i % 2) * placeGroup.size()) as Float;
            }
            distIdMap.balance(locality, mm);
            mm.sync();
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.printLocality();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-1: FAIL");
	}

        // ReDistribute all entries using balance() Pattern 2
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 2");
	placeGroup.broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
            val locality = new Rail[Float](placeGroup.size());
            for (i in 0..(placeGroup.size() - 1)) {
                locality(i) = 1.0f;
            }
            distIdMap.balance(locality, mm);
            mm.sync();
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.printLocality();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

        // ReDistribute all entries using balance() Pattern 3
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 3");
	placeGroup.broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
            val locality = new Rail[Float](placeGroup.size());
            for (i in 0..(placeGroup.size() - 1)) {
                if (i % 2 == 0) {
                    locality(i) = 1.0f;
                } else {
                    locality(i) = 0f;
                }
            }
            distIdMap.balance(locality, mm);
            mm.sync();
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.printLocality();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-1: FAIL");
	}
        
    }
}
