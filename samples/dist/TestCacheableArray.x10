package samples.dist;

import x10.util.*;
import cassia.dist.*;
import cassia.util.*;

public class TestCacheableArray {

    protected val messages: PlaceLocalHandle[ArrayList[String]];
    val placeGroup: PlaceGroup;
    val team: Team;

    val size = 10;


    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
        messages = PlaceLocalHandle.make[ArrayList[String]](placeGroup, () => {return new ArrayList[String]();});
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new TestCacheableArray(pg, team).run();
    }

    public def gatherMessages() {

/*
        val printMessages = (tag: String, msgs: ArrayList[String]) => {
	    for (i in 0..(msgs.size() - 1)) {
	    	Console.OUT.println("" + tag + ": " + msgs(i));
	    }
	};

	for (i in 0..(placeGroup.numPlaces() - 1)) {
	    at (placeGroup(i)) printMessages("Place " + i, messages());
	}
*/

	placeGroup.broadcastFlat(() => {

	    val sb = new StringBuilder();
	    for (s in messages()) {
	        sb.add(s);
	    	sb.add("\n");
            }
	    val bytes = sb.toString().bytes();
//	    Console.OUT.println(sb);
//	    Console.OUT.println(bytes.size);

            team.barrier();
	    val root = placeGroup(0);
	    val nplaces = placeGroup.numPlaces();	    
	    val dcounts = new Rail[Int](nplaces);
	    val msglen: Int = bytes.size as Int;
	    val tmpCounts = new Rail[Int](nplaces, msglen);
	    team.alltoall(tmpCounts, 0, dcounts, 0, 1);

/*
	    if (here == root) {
	        for (i in 0..(nplaces - 1)) {
	           Console.OUT.println("dcounts(" + i + ") = " + dcounts(i));
                }
            }
*/

	    var offset :Long = 0;
	    var total :Long = 0;
	    for (i in 0..(nplaces - 1)) {
	        if (i < here.id) {
		    offset = offset + dcounts(i);
		}
		total = total + dcounts(i);
	    }
	    val allmsgs = new Rail[Byte](total);

	    team.gatherv(root, bytes, 0, allmsgs, offset, dcounts); 

	    if (here == root) {
	        Console.OUT.println("all messages received:");
	        Console.OUT.println(new String(allmsgs));
 	    }

	    // Clear messages
	    messages().clear();

	    team.barrier();
        });        


    }

    public def printLocal[T](tag: String, collection: Collection[T]) {
        Console.OUT.print(tag + ": [");
        for (elem in collection) {
	    Console.OUT.print("" + elem + ", ");
	}
	Console.OUT.println("]");
    }

    public def printLocalHistory[T](tag: String, withHistory: WithHistory[T]) {
	val sb = new StringBuilder();
        sb.add(tag + ": [");
	for (elem in withHistory.getHistory()) {
	    sb.add("" + elem + ", ");
        }
	sb.add("" + withHistory() + "]");
	messages().add(sb.toString());
//	Console.OUT.println(sb.toString());
    }

    public def print[T](collection: Collection[T]) {
        Console.OUT.println();
        Console.OUT.println("# print the string at all places");
	for (p in Place.places()) at (p) {
	    for (elem in collection) {
	        Console.OUT.println("" + elem.toString() + " @ " + here);
    	    }
	}
        Console.OUT.println();
    }

    public def run(): void {
	val pg = placeGroup;
	val original = new ArrayList[WithHistory[String]]();

	for (i in 0..(size - 1)) {
	    original.add(new WithHistory[String]("instance(" + i + ") created @" + here));
	}

        val colDist = new CacheableArray[WithHistory[String]](pg, team, original);

//	print(colDist);

        Console.OUT.println("# initialize colDist");

//        Console.OUT.println("distIdMap(here.id) = here.toString();");

        pg.broadcastFlat(() => {
//            distIdMap(here.id) = here.toString();
//	    printLocal("dist@"+here, colDist);
	    for (i in 0..(size - 1)) {
  	        printLocalHistory("colDist@" + here + "(" + i + ")", colDist(i));
            }
//	    team.barrier();
//	    Console.OUT.println("messages.size = " + messages().size());
        });

	gatherMessages();
	
        // put a new entry at each place
	Console.OUT.println();
        Console.OUT.println("# put new data into colDist");

        pg.broadcastFlat(() => {
	    var counter: Long = 0;

	    for (i in 0..(size - 1)) {
	        colDist(i)() = "instance(" + i + ") = " + counter * (here.id + 1) + " @" + here;
		counter = counter + 1;
    	    }
	
	    for (i in 0..(size - 1)) {
  	        printLocalHistory("dist@" + here + "(" + i + ")", colDist(i));
            }
//	    printLocal("dist@"+here, colDist);
        });

	gatherMessages();

	// update
	Console.OUT.println();
        Console.OUT.println("# broadcast colDist");

        pg.broadcastFlat(() => {
	    colDist.broadcast(UpdateString.pack, UpdateString.unpack);

	    for (i in 0..(size - 1)) {
  	        printLocalHistory("dist@" + here + "(" + i + ")", colDist(i));
            }
//	    printLocal("dist@"+here, colDist);
        });

	gatherMessages();

/*
	Console.OUT.println();
        Console.OUT.println("# check local history");
        pg.broadcastFlat(() => {
	    for (i in 0..(size - 1)) {
  	        printLocalHistory("dist@" + here + "(" + i + ")", colDist(i));
            }
        });
*/

    }
}

struct UpdateString(newString: String) {
   static val pack = (container: WithHistory[String]): UpdateString => {
       return UpdateString(container());
   };

   static val unpack = (container: WithHistory[String], update: UpdateString): void => {
       container() = update.newString;
   };
}
