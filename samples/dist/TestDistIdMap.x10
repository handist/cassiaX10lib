package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestDistIdMap {

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
        new TestDistIdMap(pg, team).run();
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

	// ---------------------------------------------------------------------------

        // Move all entries to the next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place.places().next(here);
	    distIdMap.each((key: Long, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distIdMap.moveAtSync(key, destination, mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = (key + 1) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next place");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 2-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-2: FAIL");
	}

	// ---------------------------------------------------------------------------

        // Move all entries to the next to next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next to next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place.places().next(here);
	    distIdMap.each((key: Long, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distIdMap.moveAtSync(key, destination, mm);
	    });
            mm.sync();
	    distIdMap.each((key: Long, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distIdMap.moveAtSync(key, destination, mm);
	    });
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = (key + 3) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next to next place");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 3-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-2: FAIL");
	}

	// ---------------------------------------------------------------------------

        // Move all entries to the NPLACES times next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the NPLACES times next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place.places().next(here);

	    for (i in 1..NPLACES) {
	        distIdMap.each((key: Long, value: String) => {
		    Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();
	    }
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = (key + 3) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the NPLACES times next place");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 4-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-2: FAIL");
	}

	// ---------------------------------------------------------------------------

        // Move all entries to place 0
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to place 0");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place(0);
	    distIdMap.each((key: Long, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distIdMap.moveAtSync(key, destination, mm);
	    });
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		return 0n;
	    })) {
	    Console.OUT.println("VALIDATE 5-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 5-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to place 0");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 5-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 5-2: FAIL");
	}

	// ---------------------------------------------------------------------------

	// Generate additional key/value pair

	for (i in numData..(numData * 2 - 1)) {
	    distIdMap(i) = genRandStr("v");
	}

	// Distribute all entries with additional key/value
        Console.OUT.println("");
        Console.OUT.println("### Distribute all entries with additional key/value");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distIdMap.each((key: Long, value: String) => {
		val d = key % NPLACES;
		Console.OUT.println("" + here + " moves key: " + key + " to " + d);
		distIdMap.moveAtSync(key, placeGroup(d), mm);
	    });

            mm.sync();
        });

	// Then remove additional key/value
        Console.OUT.println("");
        Console.OUT.println("### Then remove additional key/value");
        Place.places().broadcastFlat(() => {
	    val keyList = new ArrayList[Long]();
	    distIdMap.each((key: Long, value: String) => {
	        if (key >= numData) {
		    Console.OUT.println("[" + here.id + "] try to remove " + key);
		    keyList.add(key);
		}
	    });
	    for (key in keyList) {
	        distIdMap.remove(key);
	    }
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = key % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 6-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 6-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Distribute all entries again and remove additional data");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 6-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 6-2: FAIL");
	}

    }
}
