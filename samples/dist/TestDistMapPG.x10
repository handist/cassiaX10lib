package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestDistMapPG {

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
        new TestDistMapPG(pg, team).run();
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
	    distMap.each((key: String, value: String) => {
		val h = key.hashCode() as Long;
		val d = Math.abs(h) % NPLACES;
		Console.OUT.println("" + here + " moves key: " + key + " to " + d);
		distMap.moveAtSync(key, placeGroup(d), mm);
	    });

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

	// Test sub Team (odd/even)

	val placesA = new Rail[Place](NPLACES / 2);
	val placesB = new Rail[Place](NPLACES / 2);
	for (i in 0..(NPLACES - 1)) {
	    if (i % 2 == 0) {
	        placesA(i / 2) = placeGroup(i);
	    } else {
	        placesB(i / 2) = placeGroup(i);
	    }
	}
	val placeGroupA = new SparsePlaceGroup(placesA);
	val placeGroupB = new SparsePlaceGroup(placesB);
	val teamA = new Team(placeGroupA);
	val teamB = new Team(placeGroupB);
	
        // Move all entries to the next place in sub-team
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next place in sub-team");
	
        Place.places().broadcastFlat(() => {
	    Console.OUT.println("[" + here.id + "] broadcast");
	    val color = here.id % 2 + 1;
//	    val role = here.id / 2;
	    val placeGroupSplit = (color == 1) ? placeGroupA : placeGroupB;
//	    val teamSplit = team.split(color as Int, role);
	    val teamSplit = (color == 1) ? teamA : teamB;

	    val mm = new MoveManagerLocal(placeGroupSplit, teamSplit);
	    val destination = placeGroupSplit.next(here);
	    distMap.each((key: String, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distMap.moveAtSync(key, destination, mm);
	    });

	    Console.OUT.println("[" + here.id + "] do sync");
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d = (Math.abs(h) + 2) % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 2-1: FAIL");
	}


	// Test sub Team (half)

	val placesL = new Rail[Place](NPLACES / 2);
	val placesH = new Rail[Place](NPLACES / 2);
	for (i in 0..(NPLACES - 1)) {
	    if (i < NPLACES / 2) {
	        placesL(i) = placeGroup(i);
	    } else {
	        placesH(i - NPLACES / 2) = placeGroup(i);
	    }
	}
	val placeGroupL = new SparsePlaceGroup(placesL);
	val placeGroupH = new SparsePlaceGroup(placesH);
	val teamL = new Team(placeGroupL);
	val teamH = new Team(placeGroupH);
	
        // Move all entries to the next place in sub-team
        Console.OUT.println("");
        Console.OUT.println("### MoveAtSync // Move all entries to the next place in sub-team");
	
        Place.places().broadcastFlat(() => {
	    Console.OUT.println("[" + here.id + "] broadcast");
	    val placeGroupSplit = (here.id < NPLACES / 2) ? placeGroupL : placeGroupH;
	    val teamSplit = (here.id < NPLACES / 2) ? teamL : teamH;

	    val mm = new MoveManagerLocal(placeGroupSplit, teamSplit);
	    val destination = placeGroupSplit.next(here);
	    distMap.each((key: String, value: String) => {
		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
		distMap.moveAtSync(key, destination, mm);
	    });

	    Console.OUT.println("[" + here.id + "] do sync");
            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: String, pid: Int) => {
		val h = key.hashCode() as Long;
		val d1 = (Math.abs(h) + 2) % NPLACES;
		val d2 = (d1 < NPLACES / 2) ? (d1 + 1) % (NPLACES / 2) : (d1 + 1) % (NPLACES / 2) + NPLACES / 2;
		return d2 as Int;
	    })) {
	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE 3-1: FAIL");
	}

    }
}
