package samples.relocate;

import x10.util.Map;
import cassia.dist.DistIdMap;
import cassia.dist.MoveManagerLocal;


public class RelocateSample {

    public static def main(args: Rail[String]): void {
        new RelocateSample().run();
    }
    public def run(): void {
	runDistIdMap();
	runDistCol();
    }
    public def runDistCol() {

    }
    public def runDistIdMap(): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        val distIdMap = new DistIdMap[String](pg,team);

        val print = () => {
            Console.OUT.println();
            Console.OUT.println("# print the string at all places");
            for (p in Place.places()) at (p) {
		distIdMap.each((k:Long, str: String) => {
                    Console.OUT.println(""+k+":"+str + " @ " + here);
                });
            }
            Console.OUT.println();
        };
	val printDist = (tag:String, dist:Map[Long,Place]) => {
	    Console.OUT.print(tag +": [");
	    for(pair in dist.entries()) {
		Console.OUT.print(""+pair.getKey()+":"+pair.getValue() +",");
	    }
	    Console.OUT.println("]");
	};
	
        // put a new entry at each place
        Console.OUT.println("# initialize distIdMap");
        Console.OUT.println("distIdMap(here.id) = here.toString();");
        pg.broadcastFlat(() => {
            distIdMap(here.id) = here.toString();
	    printDist("dist@"+here, distIdMap.getDist());
        });

        print();

        // relocate the entries to the next place
        Console.OUT.println("# all entries are transferred to the next place");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(pg,team);
            val destination = Place.places().next(here);
            for (id in distIdMap.idSet()) {
                distIdMap.moveAtSync(id, destination,mm);
            }
            mm.sync();
	    printDist("dist@"+here, distIdMap.getDist());
        });

        Place.places().broadcastFlat(() => {
		distIdMap.updateDist();
		printDist("disttAfter@"+here, distIdMap.getDist());
	    });
	
        print();

        // relocate the entries to Place(0)
        Console.OUT.println("# all entries are transferred to Place(0)");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(pg,team);
            distIdMap.each((id: Long, String) => {
		distIdMap.moveAtSync(id, Place(0),mm);
            });
            mm.sync();
        });

        Place.places().broadcastFlat(() => {
	printDist("distt2Before@"+here, distIdMap.getDist());
		distIdMap.updateDist();
		printDist("distt2After@"+here, distIdMap.getDist());
	    });
        print();
    }
}
