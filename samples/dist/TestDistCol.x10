package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import cassia.dist.*;
import cassia.util.*;

public class TestDistCol {

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
        new TestDistCol(pg, team).run();
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

        // Move all entries to the next to next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next to next place");
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
		val d = (cblock + 3) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the next to next place");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
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
	        distCol.eachChunk((c: RangedList[String]) => {
	            val r = c.getRange();
		    val cs = new ArrayList[RangedList[String]]();
		    cs.add(c);
		    Console.OUT.println("[" + r.min + ".." + r.max + "] to " + destination);
		    distCol.moveAtSync(cs, destination, mm);
	        });
                mm.sync();
	    }
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val cblock = key / (rangeSize + rangeSkip);
		val d = (cblock + 3) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to the NPLACES times next place");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
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
		return 0n;
    	})) {
	    Console.OUT.println("VALIDATE 5-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 5-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Move all entries to place 0");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
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
	for (i in numChunk..((numChunk * 2) - 1)) {
	    rangeEnd = rangeBegin + rangeSize - 1;
	    val c = new Chunk[String](rangeBegin..rangeEnd, "<empty>");
	    for (j in rangeBegin..rangeEnd) {
	        c.set("" + j + "/" + i, j);
	    }
	    distCol.putChunk(c);
	    rangeBegin = rangeBegin + rangeSize + rangeSkip;
	}

	// Distribute all entries with additional key/value
        Console.OUT.println("");
        Console.OUT.println("### Distribute all entries with additional key/value");
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

	// Then remove additional key/value
        Console.OUT.println("");
        Console.OUT.println("### Then remove additional key/value");
        Place.places().broadcastFlat(() => {
	    val chunkList = new ArrayList[RangedList[String]]();
	    distCol.eachChunk((c: RangedList[String]) => {
	        val r = c.getRange();
		if (r.min / (rangeSize + rangeSkip) >= numChunk) {
		    chunkList.add(c);
		}
	    });	    
	    for (chunk in chunkList) {
	        distCol.removeChunk(chunk);
	    }
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val cblock = key / (rangeSize + rangeSkip);
		val d = cblock % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 6-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 6-1: FAIL");
	}

        Console.OUT.println("");
        Console.OUT.println("### Update dist // Distribute all entries again and remove additional data");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val cblock = key / (rangeSize + rangeSkip);
		val d = cblock % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 6-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 6-2: FAIL");
	}

	// ---------------------------------------------------------------------------
	// split range into large pieces
	val splitSizeLarge = rangeSize * (numChunk / 3);
	val AllRange = 0..((rangeSize + rangeSkip) * numChunk - 1);

        Console.OUT.println("");
        Console.OUT.println("### Split range into large pieces splitSizeLarge: " + splitSizeLarge);
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    var range: LongRange = 0..(splitSizeLarge - 1);
	    var dest: Long = 0;
	    while (range.min <= AllRange.max) {
	        distCol.moveAtSync(range, placeGroup(dest), mm);
		range = (range.min + splitSizeLarge)..(range.max + splitSizeLarge);
		dest = (dest + 1) % NPLACES;
	    }
            mm.sync();
        });
	
	gather.gather();
	gather.print();

	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val d = (key / splitSizeLarge) % NPLACES;	    
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 7-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 7-1: FAIL");
	}


        Console.OUT.println("");
        Console.OUT.println("### Update dist // Split range into large pieces");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 7-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 7-2: FAIL");
	}

	// ---------------------------------------------------------------------------
	// split range into small pieces
	val splitSizeSmall = 4;

        Console.OUT.println("");
        Console.OUT.println("### Split range into small pieces splitSizeSmall: " + splitSizeSmall);
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    var range: LongRange = 0..(splitSizeSmall - 1);
	    var dest: Long = 0;
	    while (range.min <= AllRange.max) {
	        distCol.moveAtSync(range, placeGroup(dest), mm);
		range = (range.min + splitSizeSmall)..(range.max + splitSizeSmall);
		dest = (dest + 1) % NPLACES;
	    }
            mm.sync();
        });
	
	gather.gather();
	gather.print();

	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	        val d = (key / splitSizeSmall) % NPLACES;	    
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 8-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 8-1: FAIL");
	}


        Console.OUT.println("");
        Console.OUT.println("### Update dist // Split range into small pieces");
	placeGroup.broadcastFlat(() => {
	    distCol.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE 8-2: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 8-2: FAIL");
	}

    }
}
