package samples.dist;

import x10.util.*;
import x10.io.Serializer;
import x10.io.Deserializer;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistMapList[T, U] {T <: Comparable[T], U haszero, U <: Comparable[U]} {

    val local: PlaceLocalHandle[GatherDistMapListLocal[T, U]];
    val placeGroup: PlaceGroup;
    val root: Place;
    val nplaces: Long;
    val team: Team;

    public def this(placeGroup: PlaceGroup, distMapList: DistMapList[T, U]) {

        this.placeGroup = placeGroup;
	root = placeGroup(0);
	nplaces = placeGroup.numPlaces();
	team = new x10.util.Team(placeGroup);

        local = PlaceLocalHandle.make[GatherDistMapListLocal[T, U]](placeGroup, () => { return new GatherDistMapListLocal[T, U](distMapList); });
    }

    public def gather() {
        gatherCollection();
//	gatherDist();
    }

    public def gatherCollection() {
        placeGroup.broadcastFlat(() => {
            local().clearCollection();
	    val localDistMapList: DistMapList[T, U] = local().getDistMapList();

	    val keyValues = new ArrayList[Pair[T, U]]();
	    localDistMapList.each((k: T, v: U) => {
	        keyValues.add(Pair[T, U](k, v));
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
		local().allKeyValueLocation.clear();

	        offset = 0;
	        for (i in 0..(nplaces - 1)) {
		    val msg = new Rail[Byte](dcounts(i));
		    Rail.copy(allmsgs, offset, msg, 0, dcounts(i) as Long);
		    offset = offset + dcounts(i);

		    val dser = new Deserializer(msg);
		    val listKeyValues = dser.readAny() as ArrayList[Pair[T, U]];

		    val hashKeyValues = new HashMap[T, ArrayList[U]]();
		    for (kv in listKeyValues) {

		        val k = kv.first;
			val v = kv.second;

		    	if (hashKeyValues.containsKey(k)) {
			    hashKeyValues(k).add(v);
			} else {
			    val list = new ArrayList[U]();
			    list.add(v);
			    hashKeyValues.put(k, list);
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
			val vlist = entry.getValue();
			vlist.sort();
			for (v in vlist)  {
		    	    if (local().allValues.containsKey(k)) {
//			        Console.OUT.println("allValues put " + k + ": " + v + " on " + i);
			        local().allValues(k).add(v);
			    } else {
//			        Console.OUT.println("allValues put " + k + ": " + v + " on " + i);
			        val list = new ArrayList[U]();
			        list.add(v);
			        local().allValues.put(k, list);
			    }
			    if (local().allKeyValueLocation.containsKey(Pair[T, U](k, v))) {
			        Console.OUT.println("Error: Duplicate key: " + k + " value: " + v + " pair for allKeyValueLocation");
			        throw new Exception("Error: Duplicate key: " + k + " value: " + v + " pair for allKeyValueLocation");
			    } else {
//			        Console.OUT.println("allKeyValueLocation put " + k + ", " + v + " hashCode: " + Pair[T,U](k,v).hashCode()  + " on " + i);
			        local().allKeyValueLocation.put(Pair[T, U](k, v), i as Int);
//			        Console.OUT.println("allKeyValueLocation get " + k + ", " + v + " hashCode: " + Pair[T,U](k,v).hashCode()  + " on " + local().allKeyValueLocation.get(Pair[T, U](k, v)));
			    }
			}
		    }
		}

		for (entry in local().allValues.entries()) {
		    val list = entry.getValue();
		    list.sort();
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
	    val localDistMapList: DistMapList[T, U] = local().getDistMapList();
	    val data = localDistMapList.getDist();
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
		    Console.OUT.print("[" + i + "] DistMapList[" + k + "] = [");
		    val values = local().gatheredCollection.get(i as Int).get(k);
		    for (v in values) {
		        Console.OUT.print(v + ", ");
		    }
		    Console.OUT.println("]");
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
		    Console.OUT.print("[" + i + "] DistMapList.Dist[" + k + "] = ");
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
	    local().initValues.put(entry.getKey(), entry.getValue().clone());
	}
	local().initKeyValueLocation.clear();
	for (entry in local().allKeyValueLocation.entries()) {
	    local().initKeyValueLocation.put(entry.getKey(), entry.getValue());
	}
    }


    public def validateLocationOfKeyValue(vector: (T, U, Int)=>Int): Boolean {

/*
        for (entry in local().allKeyValueLocation.entries()) {
	    val keyvalue = entry.getKey();
	    val location = entry.getValue();
	    val key = keyvalue.first;
	    val value = keyvalue.second;
	    Console.OUT.println("## " + key + ", " + value + ", " + location + " hashCode: " + Pair[T,U](key, value).hashCode() + " got " + local().allKeyValueLocation.get(Pair[T, U](key, value)));
	}
*/

        for (entry in local().allKeyValueLocation.entries()) {
	    val keyvalue = entry.getKey();
	    val location = entry.getValue();
	    val key = keyvalue.first;
	    val value = keyvalue.second;
	    val currentLocation = local().allKeyValueLocation.get(Pair[T, U](key, value));
	    val initLocation = local().initKeyValueLocation.get(Pair[T, U](key, value));
	    val estimatedLocation = vector(key, value, initLocation);
	    if (estimatedLocation != currentLocation) {
	        Console.OUT.println("VALIDATE ERROR: key: " + key + " value: " + value + " hashCode: " + Pair[T,U](key,value).hashCode() +" moved " + currentLocation + ", estimated " + estimatedLocation);
		return false;
	    }
	}


/*
	for (entry in local().initKeyValueLocation.entries()) {
	    val keyvalue = entry.getKey();
	    val key = keyvalue.first;
	    val value = keyvalue.second;
	    Console.OUT.println("VALIDATE CHECK allKeyValueLocation " + local().allKeyValueLocation.hashCode());
	        Console.OUT.println("VALIDATE: key: " + key + " value: " + value + " hashCode: " + Pair[T,U](key,value).hashCode() +" moved " + local().allKeyValueLocation.get(Pair[T, U](key, value)) as Long);
	    val location = entry.getValue();
	    val estimatedLocation = vector(key, value, location);
	    val currentLocation = local().allKeyValueLocation.get(Pair[T, U](key, value));
	        Console.OUT.println("VALIDATE: key: " + key + " value: " + value + " hashCode: " + Pair[T,U](key,value).hashCode() +" moved " + currentLocation + "/" + local().allKeyValueLocation.get(Pair[T, U](key, value)) +  ", estimated " + estimatedLocation);
	    if (estimatedLocation != currentLocation) {
	        Console.OUT.println("VALIDATE ERROR: key: " + key + " value: " + value + " hashCode: " + Pair[T,U](key,value).hashCode() +" moved " + currentLocation + "/" + local().allKeyValueLocation.get(Pair[T, U](key, value)) +  ", estimated " + estimatedLocation);

        for (entry2 in local().allKeyValueLocation.entries()) {
	    val keyvalue2 = entry2.getKey();
	    val location2 = entry2.getValue();
	    val key2 = keyvalue2.first;
	    val value2 = keyvalue2.second;
	    Console.OUT.println("VALIDATE CHECK allKeyValueLocation " + local().allKeyValueLocation.hashCode());
	    Console.OUT.println(">> " + key2 + ", " + value2 + ", " + location2 + " got " + local().allKeyValueLocation.get(Pair[T, U](key2, value2)));
	}
	        return false;

	    }

	}
*/

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
	    val allValueList = local().allValues.get(k);
	    val initValueList = local().initValues.get(k);
	    if (allValueList.size() != initValueList.size()) {
	        Console.OUT.println("VALIDATE: allValueList.size() " + allValueList.size() + " != initValueList.size() " + initValueList.size());
	        return false;
	    }
	    val allValueListIt = allValueList.iterator();
	    val initValueListIt = initValueList.iterator();
	    while (allValueListIt.hasNext()) {
	        val allValue = allValueListIt.next();
		val initValue = initValueListIt.next();
	        if (!allValue.equals(initValue)) {
		    Console.OUT.println("VALIDATE: allValue " + allValue + " != initValue " + initValue);
		    return false;
	        }
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

