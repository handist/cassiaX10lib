package samples.dist;

import x10.util.*;
import x10.io.Serializer;
import x10.io.Deserializer;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistIdMap[U] {U haszero} {

    val local: PlaceLocalHandle[GatherDistIdMapLocal[U]];
    val placeGroup: PlaceGroup;
    val root: Place;
    val nplaces: Long;
    val team: Team;

    public def this(placeGroup: PlaceGroup, distIdMap: DistIdMap[U]) {

        this.placeGroup = placeGroup;
	root = placeGroup(0);
	nplaces = placeGroup.numPlaces();
	team = new x10.util.Team(placeGroup);

        local = PlaceLocalHandle.make[GatherDistIdMapLocal[U]](placeGroup, () => { return new GatherDistIdMapLocal[U](distIdMap); });
    }

    public def gather() {
        gatherCollection();
	gatherDist();
    }

    public def gatherCollection() {
        placeGroup.broadcastFlat(() => {
            local().clearCollection();
	    val localDistIdMap: DistIdMap[U] = local().getDistIdMap();

	    val keyValues = new ArrayList[Pair[Long, U]]();
	    localDistIdMap.each((k: Long, v: U) => {
	        keyValues.add(new Pair[Long, U](k, v));
	    });

	    val ser: Serializer = new Serializer();
	    ser.writeAny(keyValues);

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

	    Console.OUT.println("call gatherv @ " + here.id);
	    team.gatherv(root, bytes, 0, allmsgs, offset, dcounts); 
	    Console.OUT.println("done gatherv @ " + here.id);

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
		    val listKeyValues = dser.readAny() as ArrayList[Pair[Long, U]];

		    val hashKeyValues = new HashMap[Long, U]();
		    for (kv in listKeyValues) {

		        val k = kv.first;
			val v = kv.second;

		    	if (hashKeyValues.containsKey(k)) {
			    Console.OUT.println("Error: Duplicate key for hashKeyValues");
			    throw new Exception("Error: Duplicate key for hashKeyValues");
			} else {
			    hashKeyValues.put(k, v);
			}
		    }
		    if (local().gatheredCollection.containsKey(i as Int)) {
			Console.OUT.println("Error: Duplicate key for gatheredCollection");
			throw new Exception("Error: Duplicate key for gatheredCollection");
		    } else {
		        local().gatheredCollection.put(i as Int, hashKeyValues);
		    }

		    for (entry in hashKeyValues.entries()) {
		        val k = entry.getKey();
			val v = entry.getValue();
		    	if (local().allValues.containsKey(k)) {
			    Console.OUT.println("Error: Duplicate key " + k + ": " + v + " for allValues on " + i);
			    throw new Exception("Error: Duplicate key " + k + ": " + v + " for allValues on " + i);
			} else {
			    Console.OUT.println("allValues put " + k + ": " + v + " on " + i);
			    local().allValues.put(k, v);
			    local().allKeyLocation.put(k, i as Int);
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
	    val localDistIdMap: DistIdMap[U] = local().getDistIdMap();
	    val data = localDistIdMap.getDist();
	    val diff = localDistIdMap.getDiff();
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

	    Console.OUT.println("call gatherv @ " + here.id);
	    team.gatherv(root, bytes, 0, allmsgs, offset, dcounts); 
	    Console.OUT.println("done gatherv @ " + here.id);

	    if (here == root) {
//	        local().allKeys.clear();
//		local().allValues.clear();

	        offset = 0;
	        for (i in 0..(nplaces - 1)) {
		    val msg = new Rail[Byte](dcounts(i));
		    Rail.copy(allmsgs, offset, msg, 0, dcounts(i) as Long);
		    offset = offset + dcounts(i);

		    val dser = new Deserializer(msg);
		    val hashKeyPlace = dser.readAny() as HashMap[Long, Place];
		    val hashKeyInt = dser.readAny() as HashMap[Long, Int];

		    if (local().gatheredDist.containsKey(i as Int)) {
			Console.OUT.println("Error: Duplicate key for gatheredDist");
			throw new Exception("Error: Duplicate key for gatheredDist");
		    } else {
		        local().gatheredDist.put(i as Int, hashKeyPlace);
			local().gatheredDiff.put(i as Int, hashKeyInt);
		    }
		}
	    }

	});
    }

    public def print() {
        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredCollection.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[Long]();
		keys.addAll(keySet);
		keys.sort();
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistMap[" + k + "] = ");
		    val value = local().gatheredCollection.get(i as Int).get(k);
		    Console.OUT.println(value);
		}
	    }
	}

        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredDist.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[Long]();
		keys.addAll(keySet);
		keys.sort();
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistMap.Dist[" + k + "] = ");
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
            if (keySet.size() > 0) {
                for (k in keySet) {
                    val location = local().gatheredDist.get(i as Int).get(k).id;
                    numLocal(location) = numLocal(location) + 1;
                }
            }
            for (j in 0..(nplaces - 1)) {
                Console.OUT.println("[" + i + "] " + j + " has " + numLocal(j) + " / " + keySet.size());
            }
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
	    if (!allKey.equals(initKey)) {
		Console.OUT.println("VALIDATE: allKey " + allKey + " != initKey " + initKey);
		return false;
	    }
	}
	for (k in local().allKeys) {
	    val allValue = local().allValues.get(k);
	    val initValue = local().initValues.get(k);
	    if (!allValue.equals(initValue)) {
		Console.OUT.println("VALIDATE: allValue " + allValue + " != initValue " + initValue);
		return false;
	    }
	}

        // values assosiated with local keys should be exist on the local place
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
	    if (dist.size() != allKeysSize) {
	        Console.OUT.println("VALIDATE: dist size != allKeys.size()");
	        return false;
	    }
	    for (e in dist.entries()) {
	        val id = e.getKey();
		val place = e.getValue();
		if (local().allKeyLocation(id) != place.id as Int) {
		    Console.OUT.println("VALIDATE: key " + id + " location " + local().allKeyLocation(id) + " dist " + place.id);
		    return false;
		}		
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

