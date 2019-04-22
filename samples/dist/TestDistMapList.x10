package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestDistMapList {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
//    val home = PlaceGroup(0): PlaceGroup;
    val team: Team;

    val numData: Long = 200;
    val numKey: Long = 20;

    val distMapList: DistMapList[String, String];
    val keyList: ArrayList[String];
    val random: Random;

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
        this.random = new Random(12345);
	distMapList = new DistMapList[String, String](placeGroup, team);
	keyList = new ArrayList[String]();
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestDistMapList(pg, team).run();
    }


    public def genRandStr(header: String): String {
        val rand = random.nextLong();
	return header + rand.toString();
    }

    public def run(): void {

        // Create initial data at Place 0
        Console.OUT.println("### Create initial data at Place 0");

	for (i in 0..(numKey - 1)) {
	    keyList.add(genRandStr("k"));
	}

	var j: Long = 0;
	for (i in 0..(numData - 1)) {
	    distMapList.put(keyList(j), genRandStr("v"));
	    j = (j + 1) % numKey;
	}

	val gather = new GatherDistMapList[String, String](placeGroup, distMapList);
	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

        // Distribute all entries
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Distribute all entries");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distMapList.each((key: String, value: String) => {
		val h = key.hashCode() as Long;
		val d = Math.abs(h) % NPLACES;
		Console.OUT.println("" + here + " moves key: " + key + " to " + d);
		distMapList.moveAtSync(key, placeGroup(d), mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = Math.abs(h) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}

/*
        Console.OUT.println("");
        Console.OUT.println("### Update dist // Distribute all entries");
	placeGroup.broadcastFlat(() => {
	    distMapList.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate()) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}
*/

        // Move all entries to the next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place.places().next(here);
	    distMapList.each((key: String, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distMapList.moveAtSync(key, destination, mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = (Math.abs(h) + 1) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}


/*
        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next place");
	placeGroup.broadcastFlat(() => {
	    distMapList.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate()) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}
*/

	// Add new data on Place 0
	Console.OUT.println("### Add new data on Place 0");

	for (i in 0..(numData - 1)) {
	    distMapList.put(keyList(j), genRandStr("x"));
	    j = (j + 1) % numKey;
	}

	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

        // Move entries on even number place to the next odd number place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move entries on even number place to the next odd number place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place.places().next(here);
	    if (here.id % 2 == 0) {
	        distMapList.each((key: String, value: String) => {
		    Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		    distMapList.moveAtSync(key, destination, mm);
	        });
	    }
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		if (value.startsWith("v")) {
		    val h = key.hashCode() as Long;
		    val prev = (Math.abs(h) + 1) % NPLACES;
		    if (prev % 2 == 0) {
//		        Console.OUT.println("v even " + prev);
		        return ((prev + 1) % NPLACES) as Int;
		    } else {
//		        Console.OUT.println("v odd " + prev);
		        return prev as Int;
		    }
		} else {
		    // value.startsWith("x")
		    if (pid as Long % 2  == 0) {
//		        Console.OUT.println("x even " + pid);
		        val d = (pid as Long + 1) % NPLACES;
			return d as Int;
		    } else {
//		        Console.OUT.println("x odd " + pid);
		        return pid;
		    }
		}
	    })) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}

/*
        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next to next place");
	placeGroup.broadcastFlat(() => {
	    distMapList.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate()) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}
*/


        // Move all entries to place 0
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to place 0");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    val destination = Place(0);
	    distMapList.each((key: String, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distMapList.moveAtSync(key, destination, mm);
	    });
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		return 0n;
	    })) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}


/*
        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to place 0");
	placeGroup.broadcastFlat(() => {
	    distMapList.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate()) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}
*/

    }
}
