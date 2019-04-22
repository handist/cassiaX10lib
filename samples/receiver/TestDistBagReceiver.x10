package samples.receiver;
import cassia.concurrent.Pool;
import cassia.dist.DistAccumulator;
import cassia.dist.DistCol;
import cassia.dist.Receiver;
import x10.util.ArrayList;
import x10.util.List;
import x10.util.StringBuilder;
import x10.util.Team;
import x10.xrx.Runtime;

public class TestDistBagReceiver {

    static class Local {

        static val nthreads: Long = (Runtime.NTHREADS as Long);

        val source: DistCol[Long];
        val accum: DistAccumulator[Long];

        static def printElements(iterable: Iterable[Long]): void {
            val sb = new StringBuilder();
            val it = iterable.iterator();
            sb.add(here + ": [");
            if (it.hasNext()) {
                sb.add(it.next());
                while (it.hasNext()) {
                    sb.add(", " + it.next());
                }
            }
            sb.add("]");
            Console.OUT.println(sb);
        }

        def this(source: DistCol[Long], accum: DistAccumulator[Long]) {
            this.source = source;
            this.accum = accum;
        }

        def printName(): void {
            Console.OUT.println("Local");
        }

        def run(): void {
            for (1..nthreads) {
                source.add(here.id);
            }
            printElements(source);
            source.each(accum, nthreads, (value: Long, receiver: Receiver[Long]) => {
                Console.OUT.println(here + ": submitting " + (value + 1));
                receiver.receive(value + 1);
            });
            printElements(accum);
        }
    }

    static class LocalTestIterable extends Local {

        def this(source: DistCol[Long], accum: DistAccumulator[Long]) {
            super(source, accum);
        }

        def printName(): void {
            Console.OUT.println("LocalTestIterable");
        }

        def run(): void {
            for (1..nthreads) {
                source.add(here.id);
            }
            printElements(source);
            source.each(nthreads, (values: Iterable[Long]) => {
                val receiver = accum.getReceiver();
                for (value in values) {
                    receiver.receive(value + 1);
                }
                receiver.close();
            });
            printElements(accum);
        }
    }

    val placeGroup: PlaceGroup;
    val team: Team;
    val source: DistCol[Long];
    val accum: DistAccumulator[Long];

    def this(args: Rail[String]) {
        val placeGroup = Place.places();
        val team = Team.WORLD;
        this.placeGroup = placeGroup;
        this.team = team;
        val source = new DistCol[Long](placeGroup, team);
        this.source = source;
        val accum = new DistAccumulator[Long](placeGroup, team);
        this.accum = accum;
    }

    def run(): void {
        val source = this.source;
        val accum = this.accum;
        placeGroup.broadcastFlat(() => {
            // val local = new Local(source, accum);
            val local = new LocalTestIterable(source, accum);
            if (here == placeGroup(0)) {
                local.printName();
            }
            local.run();
        });
    }

    public static def main(args: Rail[String]): void { 
        // test parallel accumulation (each)
        val object = new TestDistBagReceiver(args);
        object.run();
    }
}
