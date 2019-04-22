package cassia.dist;



import x10.io.Serializer;
import x10.io.Deserializer;
import x10.util.RailBuilder;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Set;
import x10.util.HashSet;
import x10.util.Team;
import x10.util.Random;



// for internal use
// this is a class for the load balancing
final class LoadBalancer[T] {

    private static val tmpRoot = Place(0);

    private val list: List[T];
    private val pg: PlaceGroup;
    private val team: Team;
    private val root: Place;
    private val myRole: Long;
    private val senders: Set[Long];
    private val receivers: Set[Long];

    public def this(list: List[T], pg: PlaceGroup, team: Team) {
        assert pg.size() == team.size();
        this.list = list;
        this.pg = pg;
        this.team = team;
        this.root = pg(0);
        this.myRole = pg.indexOf(here);
        this.senders = new HashSet[Long](pg.size() as Int);
        this.receivers = new HashSet[Long](pg.size() as Int);
    }

    public def execute(): void {
        if (pg.size() == 1) return;
        relocate(getMoveCount());
    }

    // return (fromId, toId) => moveCount
    private def getMoveCount(): (Long,Long)=>Int {
        val np = pg.size();
        val matrix = new Rail[Int](np * np, (Long) => 0n);
        this.senders.clear();
        this.receivers.clear();
        val tmpOverCounts = new Rail[Long](np, (Long) => list.size());
        val overCounts = new Rail[Long](np);
        team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
        var total: Long = 0;
        for (i in overCounts.range()) {
            total = total + overCounts(i);
        }
        val average = total / np;
        for (i in overCounts.range()) {
            overCounts(i) = overCounts(i) - average;
        }
        for (i in overCounts.range()) {
            val overCount = overCounts(i);
            if (overCount < 0) {
                this.receivers.add(i);
            } else if (overCount > 0) {
                this.senders.add(i);
            }
        }
        if ((here == root) && (0 < this.senders.size()) && (0 < this.receivers.size())) {
            val senders = new ArrayList[Long](this.senders.size());
            val receivers = new ArrayList[Long](this.receivers.size());
            senders.addAll(this.senders);
            val random = new Random();
            for (i in 0..(senders.size() - 1)) {
                val j = random.nextLong(senders.size());
                val tmp = senders(j);
                senders(j) = senders(i);
                senders(i) = tmp;
            }
            receivers.addAll(this.receivers);
            receivers.sort((a: Long, b: Long) => (overCounts(b) - overCounts(a)) as Int);
            var senderPointer: Long = 0;
            var receiverPointer: Long = 1;
            while ((receiverPointer < receivers.size()) && (senderPointer < senders.size())) {
                val i = senders(senderPointer);
                val j = receivers(receiverPointer - 1);
                val k = receivers(receiverPointer);
                while ((overCounts(k) < overCounts(j)) && (0 < overCounts(i))) {
                    overCounts(i)--;
                    overCounts(k)++;
                    matrix(np * i + k)++;
                }
                if (overCounts(j) == overCounts(k)) {
                    receiverPointer++;
                }
                if (overCounts(i) == 0) {
                    senderPointer++;
                }
            }
            while (senderPointer < senders.size()) {
                val i = senders(senderPointer);
                while (0 < overCounts(i)) {
                    receiverPointer = (receiverPointer + 1) % receivers.size();
                    val j = receivers(receiverPointer);
                    overCounts(i)--;
                    overCounts(j)++;
                    matrix(np * i + j)++;
                }
                senderPointer++;
            }
        }
        team.bcast(tmpRoot, matrix, 0, matrix, 0, np * np);
        return (i: Long, j: Long) => matrix(np * i + j);
    }

    // execute relocation using getCount function
    private def relocate(getCount: (Long,Long)=>Int) {
        val np = team.size();
        val srcBuilder = new RailBuilder[Byte]();
        val scounts = new Rail[Int](np);
        val tmpMatrix = new Rail[Int](np * np);
        val matrix = new Rail[Int](np * np);
        for (var j: Long = 0; j < np; j++) {
            val count = getCount(myRole, j);
            val s = new Serializer();
            val tmpBuilder = new RailBuilder[T]();
            for (var k: Long = 0; k < count; k++) {
                tmpBuilder.add(list.removeLast());
            }
            s.writeAny(tmpBuilder.result());
            scounts(j) = s.dataBytesWritten();
            srcBuilder.insert(srcBuilder.length(), s.toRail());
        }
        for (var i: Long = 0; i < np; i++) {
            Rail.copy(scounts, 0, tmpMatrix, np * i, np);
        }
        team.alltoall(tmpMatrix, 0, matrix, 0, np);
        val src = srcBuilder.result();
        for (i in senders) {
            val tmpScounts = new Rail[Int](np);
            Rail.copy(matrix, np * i, tmpScounts, 0, np);
            val receiveCount = tmpScounts(myRole) as Long;
            val dst = new Rail[Byte](receiveCount);
            team.scatterv(Place(i), src, 0, dst, 0, tmpScounts);
            val ds = new Deserializer(dst);
            val received = ds.readAny() as Rail[T];
            val count = getCount(i, myRole) as Long;
            assert(received.size == count);
            for (elem in received) {
                list.add(elem);
            }
        }
    }
}

// test
class Main {

    public static def main(args: Rail[String]): void {
        val o = new Main();
        o.run();
    }

    def run(): void {
        val pg = Place.places();
        val team = Team(pg);
        pg.broadcastFlat(() => {
            val executor = new Executor(pg, team);
            executor.start();
        });
    }

    static class Executor(pg: PlaceGroup, team: Team) {

        transient var map: x10.util.Map[Long, String];

        def start(): void {
            initialize(() => 1000000);
            balance();
        }

        def initialize(count: ()=>Long): void {
            val begin = System.nanoTime();
            if (map == null) {
                map = new x10.util.HashMap[Long, String]();
            }
            val num = count();
            for (var i: Long = 0; i < num; i++) {
                map(num * here.id + i) = i.toString();
            }
            val end = System.nanoTime();
            Console.OUT.println(here + " initialize " + ((end - begin) * 1e-6) + " ms");
        }

        def balance(): void {
            val begin = System.nanoTime();
            val al = new ArrayList[x10.util.Map.Entry[Long, String]](map.size());
            al.addAll(map.entries());
            map.clear();
            val balancer = new LoadBalancer[x10.util.Map.Entry[Long, String]](al, pg, team);
            balancer.execute();
            for (e in al) {
                map(e.getKey()) = e.getValue();
            }
            val end = System.nanoTime();
            Console.OUT.println(here + " balance " + ((end - begin) * 1e-6) + " ms");
        }
    }
}
