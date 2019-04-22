package samples.dist;

import x10.util.*;
import x10.io.Serializer;
import x10.io.Deserializer;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistMap[T, U] {T <: Comparable[T], U haszero} {

    val local: PlaceLocalHandle[GatherDistMapLocal[T, U]];
    val placeGroup: PlaceGroup;
    val root: Place;
    val nplaces: Long;
    val team: Team;

    public def this(placeGroup: PlaceGroup, distMap: DistMap[T, U]) {

        this.placeGroup = placeGroup;
	root = placeGroup(0);
	nplaces = placeGroup.numPlaces();
	team = new x10.util.Team(placeGroup);

        local = PlaceLocalHandle.make[GatherDistMapLocal[T, U]](placeGroup, () => { return new GatherDistMapLocal[T, U](distMap); });
    }

    public def gather() {
        gatherCollection();
//	gatherDist();
    }

    public def gatherCollection() {
        placeGroup.broadcastFlat(() => {
            local().clearCollection();
	    val localDistMap: DistMap[T, U] = local().getDistMap();

	    val keyValues = new ArrayList[Pair[T, U]]();
	    localDistMap.each((k: T, v: U) => {
	        keyValues.add(new Pair[T, U](k, v));
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
		    val listKeyValues = dser.readAny() as ArrayList[Pair[T, U]];

		    val hashKeyValues = new HashMap[T, U]();
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

/*
    public def gatherDist() {
        placeGroup.broadcastFlat(() => {
            local().clearDist();
	    val localDistMap: DistMap[T, U] = local().getDistMap();
	    val data = localDistMap.getDist();
	    val ser: Serializer = new Serializer();
	    ser.writeAny(data);
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
		    val hashKeyPlace = dser.readAny() as HashMap[T, Place];

		    if (local().gatheredDist.containsKey(i as Int)) {
			Console.OUT.println("Error: Duplicate key for gatheredDist");
			throw new Exception("Error: Duplicate key for gatheredDist");
		    } else {
		        local().gatheredDist.put(i as Int, hashKeyPlace);
		    }
		}
	    }

	});
    }
*/

    public def print() {
        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredCollection.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[T]();
		keys.addAll(keySet);
		keys.sort();
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistMap[" + k + "] = ");
		    val value = local().gatheredCollection.get(i as Int).get(k);
		    Console.OUT.println(value);
		}
	    }
	}
/*
        for (i in 0..(nplaces - 1)) {
	    val keySet = local().gatheredDist.get(i as Int).keySet();
	    if (keySet.size() > 0) {
	        val keys = new ArrayList[T]();
		keys.addAll(keySet);
		keys.sort();
		for (k in keys) {
		    Console.OUT.print("[" + i + "] DistMap.Dist[" + k + "] = ");
		    val value = local().gatheredDist.get(i as Int).get(k);
		    Console.OUT.println(value);
		}
	    }
	}
*/
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


    public def validateLocationAndValue(vector: (T, Int)=>Int): Boolean {

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

/*
        // values assosiated with local keys are exist on the local place
	for (p in 0..(nplaces - 1)) {
	    val collection = local().gatheredCollection(p as Int);
	    val dist = local().gatheredDist.get(p as Int);
	    for (distEntry in dist.entries()) {
	        val distEntryKey = distEntry.getKey();
		val distEntryValue = distEntry.getValue();
		if (distEntryValue.id == p) {
		    if (!collection.containsKey(distEntryKey)) {
		        Console.OUT.println("VALIDATE: Place " + p + " does not have value for " + distEntryKey);
		        return false;
		    }
		}
	    }
	}
*/

        return true;
    }
}

