package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class TestTeam {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
    val team: Team;

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestTeam(pg, team).run();
    }

    public def run(): void {

	// Test PlaceGroup

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

        Place.places().broadcastFlat(() => {
//        placeGroupA.broadcastFlat(() => {
	    Console.OUT.println("[" + here.id + "] broadcast");
	    val color = here.id % 2 + 1;
	    val role = here.id / 2;
//	    val placeGroupSplit = (color == 1) ? placeGroupA : placeGroupB;
//	    val teamSplit = team.split(color as Int, role);
	    val teamSplit = (color == 1) ? teamA : teamB;

	    Console.OUT.println("[" + here.id + "] barrier in");
//	    teamSplit.barrier();
	    if (color ==1) {
	        teamA.barrier();
  	    }
	    Console.OUT.println("[" + here.id + "] barrier out");

	});


        Console.OUT.println("# TestTeam");
/*
        Place.places().broadcastFlat(() => {
	    Console.OUT.println("[" + here.id + "] barrier in");
	    team.barrier();
	    Console.OUT.println("[" + here.id + "] barrier out");
        });
*/


        Place.places().broadcastFlat(() => {
        });


    }
}
