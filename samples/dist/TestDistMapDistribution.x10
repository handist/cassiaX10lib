package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestDistMapDistribution {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
//    val home = PlaceGroup(0): PlaceGroup;
    val team: Team;

    val numData: Long = 200;

    val distMap: DistMap[String, String];
    val random: Random;

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
        this.random = new Random(12345);
	distMap = new DistMap[String, String](placeGroup, team);
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestDistMapDistribution(pg, team).run();
    }


    public def genRandStr(header: String): String {
        val rand = random.nextLong();
	return header + rand.toString();
    }

    public def run(): void {

        // Create initial data at Place 0
        Console.OUT.println("### Create initial data at Place 0");

	for (i in 0..(numData - 1)) {
	    distMap(genRandStr("k")) = genRandStr("v");
	}

	val gather = new GatherDistMap[String, String](placeGroup, distMap);
	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

        // Distribute all entries
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Distribute all entries");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distMap.moveAtSync((key: String) => {
		val h = key.hashCode() as Long;
		val d = Math.abs(h) % NPLACES;
	        return placeGroup(d);
	    }, mm);

            mm.sync();
        });


	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = Math.abs(h) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 1-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 1-1: FAIL");
	}


        // Move all entries to the next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distMap.moveAtSync((key: String) => {
	        return Place.places().next(here);
	    }, mm);
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = (Math.abs(h) + 1) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-1: FAIL");
	}


        // Move all entries to the next to next place
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next to next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distMap.moveAtSync((key: String) => {
	        return Place.places().next(here);
	    }, mm);
            mm.sync();
	    distMap.moveAtSync((key: String) => {
	        return Place.places().next(here);
	    }, mm);
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = (Math.abs(h) + 3) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

        // Move all entries to place 0
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to place 0");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distMap.moveAtSync((key: String) => {
	        return Place.places()(0);
	    }, mm);
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		return 0n;
	    })) {
	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 4-1: FAIL");
	}


    }
}
