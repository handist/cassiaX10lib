package samples.dist;

import x10.util.*;
import x10.io.Serializer;
import x10.io.Deserializer;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistCol[T] {T haszero} {

    val local: PlaceLocalHandle[GatherDistColLocal[T]];
    val placeGroup: PlaceGroup;
    val root: Place;
    val nplaces: Long;
    val team: Team;

    public def this(placeGroup: PlaceGroup, distCol: DistCol[T]) {

        this.placeGroup = placeGroup;
	root = placeGroup(0);
	nplaces = placeGroup.numPlaces();
	team = new x10.util.Team(placeGroup);

        local = PlaceLocalHandle.make[GatherDistColLocal[T]](placeGroup, () => { return new GatherDistColLocal[T](distCol); });
    }

    public def gather() {
        gatherCollection();
	gatherDist();
    }

    public def gatherCollection() {
        placeGroup.broadcastFlat(() => {
            local().clearCollection();
	    val localDistCol: DistCol[T] = local().getDistCol();
	    val chunks = new ArrayList[RangedList[T]]();
	    localDistCol.eachChunk((c: RangedList[T]) => {
//	        Console.OUT.print("[" + here.id + "] gatherCollection chunks.add " + c.getRange() + " ");
//		if (c instanceof Chunk[T]) {
//		    Console.OUT.println("Chunk size: " + c.toRail().size);
//		} else if (c instanceof RangedListView[T]) {
//		    Console.OUT.println("RangedListView size: " + c.toRail().size);
//		}
	        chunks.add(c);
	    });	    	    
	    val ser: Serializer = new Serializer();
//	    Console.OUT.println("before call writeAny");
	    ser.writeAny(chunks);
//	    Console.OUT.println("before call toRail");
	    val bytes = ser.toRail();
//	    Console.OUT.println("toRail done");
	    val dcounts = new Rail[Int](nplaces);
	    val msglen: Int = bytes.size as Int;
	    val tmpCounts = new Rail[Int](nplaces, msglen);
	    team.alltoall(tmpCounts, 0, dcounts, 0, 1);

	    var offset :Long = 0;
	    var total :Long = 0;
	    for (i in 0..(nplaces - 1)) {
	        if (i < here.id) {
		    offset = offset + dcounts(i);
		}
		total = total + dcounts(i);
	    }
	    val allmsgs = new Rail[Byte](total);

//	    Console.OUT.println("call gatherv @ " + here.id);
	    team.gatherv(root, bytes, 0, allmsgs, offset, dcounts); 
//	    Console.OUT.println("done gatherv @ " + here.id);

	    if (here == root) {
	        local().allKeys.clear();
		local().allValues.clear();
		local().allKeyLocation.clear();

	        offset = 0;
	        for (i in 0..(nplaces - 1)) {
		    val msg = new Rail[Byte](dcounts(i));
		    Rail.copy(allmsgs, offset, msg, 0, dcounts(i) as Long);
		    offset = offset + dcounts(i);

		    val dser = new Deserializer(msg);
		    val listRangedList = dser.readAny() as List[RangedList[T]];
		    val hashRangeT = new HashMap[LongRange, Rail[T]]();
		    for (rl in listRangedList) {
		        val range = rl.getRange();

//	    Console.OUT.println("before call toRail 2");
			val values = rl.toRail();
//	    Console.OUT.println("toRail 2 done");
		    	if (hashRangeT.containsKey(range)) {
			    Console.OUT.println("Error: Duplicate key for hashRangeT");
			    throw new Exception("Error: Duplicate key for hashRangeT");
			} else {
			    hashRangeT.put(range, values);
			}
		    }
		    if (local().gatheredCollection.containsKey(i as Int)) {
			Console.OUT.println("Error: Duplicate key for gatheredCollection");
			throw new Exception("Error: Duplicate key for gatheredCollection");
		    } else {
		        local().gatheredCollection.put(i as Int, hashRangeT);
		    }

		    for (entry in hashRangeT.entries()) {
		        val k = entry.getKey();
			val v = entry.getValue();
			if (k.max - k.min + 1 != v.size) {
			    Console.OUT.println("Error: Size mismatch " + k + ": " + v.size + " on " + i);
			    throw new Exception("Error: Size mismatch " + k + ": " + v.size + " on " + i);
			}
			for (ki in k) {
		    	    if (local().allValues.containsKey(ki)) {
			        Console.OUT.println("Error: Duplicate key " + ki + ": " + v + " for allValues on " + i);
			        throw new Exception("Error: Duplicate key " + ki + ": " + v + " for allValues on " + i);
			    } else {
//			        Console.OUT.println("allValues put " + ki + ": " + v + " on " + i);
			        local().allValues.put(ki, v(ki - k.min));
				local().allKeyLocation.put(ki, i as Int);
			    }
			}
		    }
		}

		local().allKeys.addAll(local().allValues.keySet());
		local().allKeys.sort();
	    }

	});
    }

    public def gatherDist() {
        placeGroup.broadcastFlat(() => {
            local().clearDist();
	    local().clearDiff();
	    val localDistCol: DistCol[T] = local().getDistCol();
	    val data = localDistCol.getDist();
	    val diff = localDistCol.getDiff();
	    val ser: Serializer = new Serializer();
	    ser.writeAny(data);
	    ser.writeAny(diff);
	    val bytes = ser.toRail();
	    val dcounts = new Rail[Int](nplaces);
	    val msglen: Int = bytes.size as Int;
	    val tmpCounts = new Rail[Int](nplaces, msglen);
	    team.alltoall(tmpCounts, 0, dcounts, 0, 1);

	    var offset :Long = 0;
	    var total :Long = 0;
	    for (i in 0..(nplaces - 1)) {
	        if (i < here.id) {
		    offset = offset + dcounts(i);
		}
		total = total + dcounts(i);
	    }
	    val allmsgs = new Rail[Byte](total);

//	    Console.OUT.println("call gatherv @ " + here.id);
	    team.gatherv(root, bytes, 0, allmsgs, offset, dcounts); 
//	    Console.OUT.println("done gatherv @ " + here.id);

	    if (here == root) {
//	        local().allKeys.clear();
//		local().allValues.clear();

	        offset = 0;
	        for (i in 0..(nplaces - 1)) {
		    val msg = new Rail[Byte](dcounts(i));
		    Rail.copy(allmsgs, offset, msg, 0, dcounts(i) as Long);
		    offset = offset + dcounts(i);

		    val dser = new Deserializer(msg);
		    val hashRangePlace = dser.readAny() as HashMap[LongRange, Place];
		    val hashRangeInt = dser.readAny() as HashMap[LongRange, Int];

		    if (local().gatheredDist.containsKey(i as Int)) {
			Console.OUT.println("Error: Duplicate key for gatheredDist");
			throw new Exception("Error: Duplicate key for gatheredDist");
		    } else {
		        local().gatheredDist.put(i as Int, hashRangePlace);
			local().gatheredDiff.put(i as Int, hashRangeInt);
		    }
		}
	    }

	});
    }

    public def print() {
        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredCollection.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[LongRange]();
		keys.addAll(keySet);
		keys.sort((range1: LongRange, range2: LongRange) => {return (range1.min - range2.min) as Int;});
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistCol[" + k + "] = [");
		    val values = local().gatheredCollection.get(i as Int).get(k);
		    for (v in values) {
		        Console.OUT.print("" + v + ", ");
		    }
		    Console.OUT.println("]");
		}
	    }
	}
        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredDist.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[LongRange]();
		keys.addAll(keySet);
		keys.sort((range1: LongRange, range2: LongRange) => {return (range1.min - range2.min) as Int;});
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistCol.Dist[" + k + "] = ");
		    val value = local().gatheredDist.get(i as Int).get(k);
		    Console.OUT.println(value);
		}
	    }
	}
    }


    public def printLocality() {
        for (i in 0..(nplaces - 1)) {
            val keySet = local().gatheredDist.get(i as Int).keySet();
            val numLocal: Rail[Long] = new Rail[Long](nplaces, 0);
            var total: Long = 0;
            if (keySet.size() > 0) {
                for (k in keySet) {
                    val location = local().gatheredDist.get(i as Int).get(k).id;
                    numLocal(location) = numLocal(location) + (k.max - k.min + 1);
                    total = total + (k.max - k.min + 1);
                }
            }
            for (j in 0..(nplaces - 1)) {
                Console.OUT.println("[" + i + "] " + j + " has " + numLocal(j) + " / " + total);
            }
        }
    }

    public def printDist(placeIdx: Long) {

	val keySet = local().gatheredDist.get(placeIdx as Int).keySet();
        val placeKeys = new ArrayList[ArrayList[LongRange]]();
        for (i in 0..(nplaces - 1)) {
            placeKeys(i) = new ArrayList[LongRange]();
        }
	if (keySet.size() > 0) {
            for (k in keySet) {
                val location = local().gatheredDist.get(placeIdx as Int).get(k).id;
                placeKeys(location).add(k);
            }
        }
        for (i in 0..(nplaces - 1)) {
	    Console.OUT.print("[" + placeIdx + "] ");
            placeKeys(i).sort((range1: LongRange, range2: LongRange) => {return (range1.min - range2.min) as Int;});
            for (r in placeKeys(i)) {
                Console.OUT.print(r.toString() + ",");
            }
	    Console.OUT.println();
        }
    }


    public def setCurrentAsInit() {
        // copy allValues/allKeys to initValues/initKeys
	local().initKeys.clear();
	local().initKeys.addAll(local().allKeys);
	local().initValues.clear();
	for (entry in local().allValues.entries()) {
	    local().initValues.put(entry.getKey(), entry.getValue());
	}
	local().initKeyLocation.clear();
	for (entry in local().allKeyLocation.entries()) {
	    local().initKeyLocation.put(entry.getKey(), entry.getValue());
	}
    }

    public def validateLocationAndValue(vector: (Long, Int)=>Int): Boolean {

	val initKeysIt = local().initKeys.iterator();
	while (initKeysIt.hasNext()) {
	    val key = initKeysIt.next();
	    val location = local().initKeyLocation(key);
	    val estimatedLocation = vector(key, location);
	    val currentLocation = local().allKeyLocation(key);
	    if (estimatedLocation != currentLocation) {
	        Console.OUT.println("VALIDATE: key " + key + " moved " + currentLocation + ", estimated " + estimatedLocation);
	        return false;
	    }
	    val initValue = local().initValues(key);
	    val currentValue = local().allValues(key);
	    if (!initValue.equals(currentValue)) {
	        Console.OUT.println("VALIDATE: value " + currentValue + " is not equal to original " + initValue);
	        return false;
	    }
	}

	return true;
    }

    public def validate(): Boolean {

        // equality allValues/allKeys and initValues/initKeys
	if (local().allKeys.size() != local().initKeys.size()) {
	    Console.OUT.println("VALIDATE: allKeys.size " + local().allKeys.size() + " != initKeys.size " + local().initKeys.size());
	    return false;
	}
	val allKeysIt = local().allKeys.iterator();
	val initKeysIt = local().initKeys.iterator();
	while (allKeysIt.hasNext()) {
	    val allKey = allKeysIt.next();
	    val initKey = initKeysIt.next();
	    if (allKey != initKey) {
		Console.OUT.println("VALIDATE: allKey " + allKey + " != initKey " + initKey);
		return false;
	    }
	}
	for (k in local().allKeys) {
	    val allValue = local().allValues.get(k);
	    val initValue = local().initValues.get(k);
	    if (!allValue.equals(initValue)) {
	        Console.OUT.println("VALIDATE: allValue(" + k + ") " + allValue + " != initValue(" + k + ") " + initValue);
	        return false;
	    }
	}

        // values assosiated with local keys are exist on the local place
	for (p in 0..(nplaces - 1)) {
	    val collection = local().gatheredCollection(p as Int);
	    val dist = local().gatheredDist.get(p as Int);
	    val diff = local().gatheredDiff.get(p as Int);
	    var distInLocal: Long = 0;
	    for (distEntry in dist.entries()) {
	        val distEntryKey = distEntry.getKey();
		val distEntryValue = distEntry.getValue();
		if (distEntryValue.id == p) {
		    distInLocal = distInLocal + 1;
		    if (!collection.containsKey(distEntryKey)) {
		        Console.OUT.println("VALIDATE: Place " + p + " does not have value for " + distEntryKey);
		        return false;
		    }
		}
	    }
//	    Console.OUT.println("distInLocal " + distInLocal);
	    if (distInLocal != collection.size()) {
	        Console.OUT.println("VALIDATE: local collection size " + collection.size() + " != dist in local size " + distInLocal);
		return false;
	    }
	    for (diffEntry in diff.entries()) {
	        val diffKey = diffEntry.getKey();
		val diffOperation = diffEntry.getValue();
		switch (diffOperation) {
		case DistManager.DIST_ADDED:
		case DistManager.DIST_MOVED_IN:
		    if (!dist.containsKey(diffKey)) {
		        Console.OUT.println("VALIDATE: Dist on Place " + p + " does not have key " + diffKey);
		        return false;
		    }
		    break;
		case DistManager.DIST_REMOVED:
		    break;
		default:
		    Console.OUT.println("VALIDATE: invalid operation in diff");		
		    return false;
		}
	    }
	}

        return true;
    }

    public def validateAfterUpdateDist() : Boolean {

        val allKeysSize = local().allKeys.size();
	for (p in 0..(nplaces - 1)) {
	    val dist = local().gatheredDist(p as Int);
	    var count :Long = 0;
	    for (e in dist.entries()) {
	        val range = e.getKey();
		val place = e.getValue();
		count = count + range.max - range.min + 1;
		for (i in range) {
		    if (local().allKeyLocation(i) != place.id as Int) {
	                Console.OUT.println("VALIDATE: key " + i + " location " + local().allKeyLocation(i) + " dist " + place.id); 
	                return false;
		    }		
		}
	    }
	    if (count != allKeysSize) {
	        Console.OUT.println("VALIDATE: dist size != allKeys.size()");
	        return false;
	    }

	    val diff = local().gatheredDiff(p as Int);
	    if (diff.size() != 0) {
	        Console.OUT.println("VALIDATE: diff size != 0");
	        return false;
	    }
	}

	return true;
    }
}

