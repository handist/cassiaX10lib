package cassia.dist;

import x10.util.Map;
import x10.util.HashMap;
import x10.util.RailBuilder;
import x10.util.RailUtils;
import x10.util.Team;
import x10.io.Serializer;
import x10.io.Deserializer;

public class CollectiveRelocator {

    private static val RELOCATE_USE_GATHERV: Boolean = (System.getenv("DIST_RELOCATE_USE_GATHERV") != null && System.getenv("DIST_RELOCATE_USE_GATHERV").equals("1"));

    public static def all2allser(placeGroup: PlaceGroup, team: Team, mm:MoveManagerLocal) throws Exception :void {
	val srcMap = new HashMap[Place, Rail[Byte]](placeGroup.size());
	for(p in placeGroup) srcMap(p)=mm.executeSerialization(p);
        val dstMap = executeRelocation(placeGroup, team, srcMap);
        mm.executeDeserialization(dstMap);
        mm.clear();
    }

    private static def executeRelocation[T](placeGroup: PlaceGroup, team: Team, srcMap:Map[Place,Rail[T]]) {T haszero}: Map[Place,Rail[T]] {
        if (RELOCATE_USE_GATHERV) {
            return collectiveRelocationUsingGatherv[T](placeGroup, team, srcMap);
        } else {
            return collectiveRelocationUsingScatterv[T](placeGroup, team, srcMap);
        }
    }

    // use scatterv
    // serial
    private static def collectiveRelocationUsingScatterv[T](placeGroup: PlaceGroup, team: Team, srcMap:Map[Place,Rail[T]]) {T haszero} {
        val numPlaces = placeGroup.numPlaces();
        val scounts = new Rail[Int](numPlaces);
        var count: Long = 0;

        for (var i: Long = 0; i < numPlaces; i++) {
            val rail = srcMap(placeGroup(i));
            scounts(i) = rail.size as Int;
            count += rail.size;
        }
        val src = new Rail[T](count);
        val scountsAll = new Rail[Int](numPlaces * numPlaces);
        var offset: Long = 0;
        for (var i: Long = 0; i < numPlaces; i++) {
            val rail = srcMap(placeGroup(i));
            Rail.copy(rail, 0, src, offset, rail.size);
            Rail.copy(scounts, 0, scountsAll, i * numPlaces, numPlaces);
            offset += rail.size;
        }
        val tmpScountsAll = new Rail[Int](numPlaces * numPlaces);
        team.alltoall(scountsAll, 0, tmpScountsAll, 0, numPlaces);
        val dstMap = new HashMap[Place,Rail[T]](numPlaces << 1);
        for (var i: Long = 0; i < numPlaces; i++) {
            val tmpScounts = new Rail[Int](numPlaces);
            Rail.copy(tmpScountsAll, numPlaces * i, tmpScounts, 0, numPlaces);
            val dst = new Rail[T](tmpScounts(placeGroup.indexOf(here)));
            team.scatterv(placeGroup(i), src, 0, dst, 0, tmpScounts);
            dstMap(placeGroup(i)) = dst;
        }
        return dstMap;
    }

    // use gatherv
    // this method is not stable because of gatherv(?).
    private static def collectiveRelocationUsingGatherv[T](placeGroup: PlaceGroup, team: Team, srcMap:Map[Place,Rail[T]]) {T haszero} {
        val numPlaces = placeGroup.numPlaces();
        val scounts = new Rail[Int](numPlaces * numPlaces);
        for (pl in placeGroup) {
            scounts(pl.id) = srcMap(pl).size as Int;
        }
        val tmpScountsAll = new Rail[Int](numPlaces * numPlaces);
        for (var i: Long = 0; i < numPlaces; i++) {
            Rail.copy(scounts, 0, tmpScountsAll, numPlaces * i, numPlaces);
        }
        val scountsAll = new Rail[Int](numPlaces * numPlaces);
        team.alltoall(tmpScountsAll, 0, scountsAll, 0, numPlaces);
        val dcountsAll = new Rail[Int](numPlaces * numPlaces);
        for (var i: Long = 0; i < numPlaces; i++) {
            for (var j: Long = 0; j < numPlaces; j++) {
                dcountsAll(numPlaces * i + j) = scountsAll(numPlaces * j + i);
            }
        }
        var receiveCount: Long = 0;
        for (var i: Long = 0; i < numPlaces; i++) {
            receiveCount += dcountsAll(numPlaces * here.id + i);
        }
        val dst = new Rail[T](receiveCount);
        for (var i: Long = 0; i < numPlaces; i++) {
            val rail = srcMap(placeGroup(i));
            val dcounts = new Rail[Int](numPlaces);
            Rail.copy(dcountsAll, numPlaces * i, dcounts, 0, numPlaces);
            team.gatherv(placeGroup(i), rail, 0, dst, 0, dcounts);
        }
        val dstMap = new HashMap[Place,Rail[T]](numPlaces << 1);
        var offset: Long = 0;
        for (var i: Long = 0; i < numPlaces; i++) {
            val bytes = new Rail[T](dcountsAll(numPlaces * here.id + i));
            Rail.copy(dst, offset, bytes, 0, bytes.size);
            dstMap(placeGroup(i)) = bytes;
            offset += bytes.size;
        }
        return dstMap;
    }

    public static def allgatherSer(pg:PlaceGroup, team:Team,  ser:(Serializer)=>void, deser:(Deserializer,Place)=>void) {
        val numPlaces = pg.numPlaces();
	val s = new Serializer();
	ser(s);
	val buf = s.toRail();
	val size = buf.size;
        val tmpCounts = new Rail[Int](numPlaces, size as Int);
        val counts = new Rail[Int](numPlaces);
        team.alltoall(tmpCounts, 0, counts, 0, 1);
        val root = pg(0);
        var total: Long = 0;
        for (c in counts) {
            total += c as Int;
        }
        val dst = new Rail[Byte](total);
        team.gatherv(root, buf, 0, dst, 0, counts);
        team.bcast(root, dst, 0, dst, 0, total);
	var offset:Long = 0;
        for (var i: Long = 0; i < numPlaces; i++) {
            val bx = new Rail[Byte](counts(i));
            Rail.copy(dst, offset, bx, 0, counts(i) as Long);
	    if(here!=pg(i)) deser(new Deserializer(bx),pg(i));
            offset += counts(i);
        }
    }

    public static def gatherSer(pg:PlaceGroup, team:Team, root:Place, ser:(Serializer)=>void, deser:(Deserializer,Place)=>void) {
        val numPlaces = pg.numPlaces();
	val s = new Serializer();
	ser(s);
	val buf = s.toRail();
	val size = buf.size;
        val tmpCounts = new Rail[Int](numPlaces, size as Int);
        val counts = new Rail[Int](numPlaces);
        team.alltoall(tmpCounts, 0, counts, 0, 1);
        var total: Long = 0;
        for (c in counts) {
            total += c as Int;
        }
        val dst = new Rail[Byte](total);
        team.gatherv(root, buf, 0, dst, 0, counts);
	if(here==root) {
	    var offset:Long = 0;
	    for (var i: Long = 0; i < numPlaces; i++) {
		val bx = new Rail[Byte](counts(i));
		Rail.copy(dst, offset, bx, 0, counts(i) as Long);
		if(here!=pg(i)) deser(new Deserializer(bx),pg(i));
		offset += counts(i);
	    }
	}
    }


    public static def bcastSer(pg:PlaceGroup, team:Team, root:Place, ser:(Serializer)=>void, deser:(Deserializer)=>void) {
        val numPlaces = pg.numPlaces();
	val tmpBuf = new Rail[Long](1);
	if(here == root) {
	    val s = new Serializer();
	    ser(s);
	    val buf = s.toRail();
	    val size = buf.size;
	    tmpBuf(0)=size;
	    team.bcast(root, tmpBuf, 0, tmpBuf, 0, 1);
	    team.bcast(root, buf, 0, buf, 0, size);
	} else {
	    team.bcast(root, tmpBuf, 0, tmpBuf, 0, 1);
	    val size = tmpBuf(0);
	    val buf = new Rail[Byte](size);
	    team.bcast(root, buf, 0, buf, 0, size);
	    val d = new Deserializer(buf);
	    deser(d);
	}
    }
}

