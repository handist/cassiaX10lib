package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import cassia.dist.*;
import cassia.util.*;

public class TestDistColBalance {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
//    val home = PlaceGroup(0): PlaceGroup;
    val team: Team;
    val rangeSize: Long = 10;
    val rangeSkip: Long = 5;
    val numChunk: Long = 50;

    val distCol: DistCol[String];
    val distBag: DistBag[List[String]];

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;

	distCol = new DistCol[String](placeGroup, team);
	distBag = new DistBag[List[String]](placeGroup, team);
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestDistColBalance(pg, team).run();
    }

    public def run(): void {

        // Create initial data at Place 0
        Console.OUT.println("### Create initial data at Place 0");

	var rangeBegin: Long = 0;
	var rangeEnd: Long;

	for (i in 0..(numChunk - 1)) {
	    rangeEnd = rangeBegin + rangeSize - 1;
	    val c = new Chunk[String](rangeBegin..rangeEnd, "<empty>");
	    for (j in rangeBegin..rangeEnd) {
	        c.set("" + j + "/" + i, j);
	    }
	    distCol.putChunk(c);
	    rangeBegin = rangeBegin + rangeSize + rangeSkip;
	}

	val gather = new GatherDistCol[String](placeGroup, distCol);
	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

	// ---------------------------------------------------------------------------

        // Distribute all entries
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Distribute all entries");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distCol.eachChunk((c: RangedList[String]) => {
	        val r = c.getRange();
		val s = c(r.min);
		val d = (Long.parse(s.split("/")(1))) % NPLACES;
		val cs = new ArrayList[RangedList[String]]();
		cs.add(c);
		Console.OUT.println("[" + r.min + ".." + r.max + "] to " + d);
		distCol.moveAtSync(cs, placeGroup(d), mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val cblock = key / (rangeSize + rangeSkip);
		val d = cblock % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 1-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 1-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Distribute all entries");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
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
	    distCol.eachChunk((c: RangedList[String]) => {
	        val r = c.getRange();
		val cs = new ArrayList[RangedList[String]]();
		cs.add(c);
		Console.OUT.println("[" + r.min + ".." + r.max + "] to " + destination);
		distCol.moveAtSync(cs, destination, mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val cblock = key / (rangeSize + rangeSkip);
		val d = (cblock + 1) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next place");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 2-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-2: FAIL");
	}

	// ---------------------------------------------------------------------------
        // ReDistribute all entries using balance() Pattern 1
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 1");
	placeGroup.broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
            val locality = new Rail[Float](placeGroup.size());
            for (i in 0..(placeGroup.size() - 1)) {
                locality(i) = (i + (i % 2) * placeGroup.size()) as Float;
            }
            distCol.balance(locality, mm);
            mm.sync();
	    distCol.updateDist();
	});

	gather.gather();
	gather.printLocality();
        gather.printDist(0);
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

	// ---------------------------------------------------------------------------
        // ReDistribute all entries using balance() Pattern 2
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 2");
	placeGroup.broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
            val locality = new Rail[Float](placeGroup.size());
            for (i in 0..(placeGroup.size() - 1)) {
                locality(i) = 1.0f;
            }
            distCol.balance(locality, mm);
            mm.sync();
	    distCol.updateDist();
	});

	gather.gather();
	gather.printLocality();
        gather.printDist(0);
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-1: FAIL");
	}
        
	// ---------------------------------------------------------------------------
        // ReDistribute all entries using balance() Pattern 3
        Console.OUT.println("");
        Console.OUT.println("### Balance & MoveAtSyncCount // Redistribute all entries Pattern 2");
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
            distCol.balance(locality, mm);
            mm.sync();
	    distCol.updateDist();
	});

	gather.gather();
	gather.printLocality();
        gather.printDist(0);
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 5-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 5-1: FAIL");
	}

    }
}
