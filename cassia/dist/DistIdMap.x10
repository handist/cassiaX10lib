package cassia.dist;

import x10.util.List;
import x10.util.ArrayList;
import x10.util.Set;
import x10.util.Map;
import x10.util.HashMap;
import x10.util.Pair;
import x10.util.RailBuilder;
import x10.util.Team;
import x10.io.Serializer;
import x10.io.Deserializer;
import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;

/**
 * Distributed Collection like Map[Long,Value].
 */
public class DistIdMap[T] {T haszero} extends DistMap[Long, T] implements ManagedDistribution[Long] {

    private static val _debug_level :Int = 0n;

    private final def getLocalInternal(): DistIdMapLocal[T] {
        return getLocal[DistIdMapLocal[T]]();
    }

    @TransientInitExpr(getLocalLDist())
    transient val ldist: DistManager.Index;
    private final def getLocalLDist(): DistManager.Index {
        val local = getLocalInternal();
        if (local == null) {
            return new DistManager.Index();
        } else {
            return local.ldist;
        }
    }

    @TransientInitExpr(getLocalLocality())
    transient val locality: Rail[Float];
    private final def getLocalLocality(): Rail[Float] {
        val local = getLocalInternal();
        if (local == null) {
            return new Rail[Float]();
        } else {
            return local.locality;
        }
    }

    public def getDist() { return ldist.dist; }
    public def getDiff() { return ldist.diff; }
    public def getDistributionLong() { return new DistributionLong(getDist()); }
    
    /**
     * Construct a DistIdMap.
     * Place.places() is used as the PlaceGroup of the new instance.
     */
    public def this() {
        this(Place.places(), Team.WORLD);
    }

    /**
     * Construct a DistIdMap with the given argument.
     * Team(placeGroup) is used as the PlaceGroup of the new instance.
     *
     * @param placeGroup the PlaceGroup.
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
        this(placeGroup, team, () => new HashMap[Long,T]());
    }

    /**
     * Construct a DistIdMap with the given argument.
     * Team(placeGroup) is used as the PlaceGroup of the new instance.
     */
    public def this(placeGroup: PlaceGroup, team: Team, init: ()=>Map[Long,T]) {
        this(placeGroup, (): Local[Map[Long, T]] => {
            val data = init();
	    val ldist = new DistManager.Index();
	    ldist.setup(data.keySet());
            val locality = new Rail[Float](placeGroup.size(), 1.0f);
            return new DistIdMapLocal[T](placeGroup, team, data, ldist, locality);
        });
    }
    
    def this(placeGroup: PlaceGroup, init: ()=>Local[Map[Long, T]]) {
        super(placeGroup, init);
        this.ldist = getLocalLDist();
        this.locality = getLocalLocality();
    }

    /**
     * Remove the all local entries.
     */
    public def clear(): void {
        super.clear();
        this.ldist.clear();
        this.locality.fill(1.0f);
    }

    /*
     * Get the corresponding value of the specified id.
     *
     * @param id a Long type value.
     * @return the corresponding value of the specified id.
     */
    public operator this(id: Long): T {
        return data(id);
    }

    /*
     * Get the corresponding value of the specified id.
     *
     * @param id a Long type value.
     * @return the corresponding value of the specified id.
     */
    public def get(id: Long): T {
        return data.get(id);
    }

    /*
     * Put a new entry.
     *
     * @param id a Long type value.
     * @param value a value.
     */
    public operator this(id: Long) = (value: T) throws Exception :T {
        return put(id, value);
    }

    /*
     * Put a new entry.
     *
     * @param id a Long type value.
     * @param value a value.
     */
    public def put(id: Long, value: T) throws Exception :T {
        if (data.containsKey(id)) {
	    return data(id) = value;
	}
	ldist.add(id);
	return data(id) = value;
    }

    private def putForMove(key: Long, keyType: Int, value: T) throws Exception :T {
        switch (keyType) {
	case DistManager.MOVE_NEW:
	    ldist.moveInNew(key);
	    break;
	case DistManager.MOVE_OLD:
	    ldist.moveInOld(key);
	    break;
	default:
	    throw new Exception("SystemError when calling putForMove " + key);
	}
	return data(key) = value;
    }
    
    public def delete(id: Long) throws Exception :Boolean {
        ldist.remove(id);
        return super.delete(id);
    }
    
    /*
     * Remove the corresponding value of the specified id.
     *
     * @param id a Long type value.
     */
    public def remove(id: Long) throws Exception :T {
        ldist.remove(id);
        return super.remove(id);
    }

    private def removeForMove(id: Long): T {
        return data.remove(id);
    }
    
    /*
     * Return true if the entry corresponding to the specified id is local.
     *
     * @return true or false.
     */
    public def containsId(id: Long): Boolean {
        return super.containsKey(id);
    }

    /*
     * Get a place where the the corresponding entry of the specified id is stored.
     * Return Place.INVALID_PLACE when it doesn't exist.
     *
     * @param id a Long type value.
     * @return the Place.
     */
    public def getPlace(id: Long): Place {
        return ldist.dist.getOrElse(id, Place.INVALID_PLACE);
    }

    /*
     * Return the Set of local ids.
     *
     * @return the Set of local ids.
     */
    public def idSet(): Set[Long] {
        return keySet();
    }

    /**
     * Execute the specified operation with the corresponding value of the specified id.
     * If the entry is stored at local, the operation is executed sequaltially.
     * If the entry is stored at a remote place, the operation is asynchronously executed at the place.
     *
     * @param id a Long type value.
     * @param op the operation.
     */
    public def execAt(id: Long, op: (T)=>void): void {
        val place = getPlace(id);
        if (place == here) {
            op(data(id));
            return;
        }
        at (place) async {
            op(data(id));
        }
    }

    public def moveAtSync(key: Long, dest: Place, mm:MoveManagerLocal) throws Exception {
        if (dest == here) return;

	val toBranch = this;
        val serialize = (s: Serializer) => {
            val value = this.removeForMove(key);
	    val keyType = ldist.moveOut(key, dest);
            s.writeAny(key);
	    s.writeAny(keyType);
            s.writeAny(value);
        };
        val deserialize = (ds: Deserializer) => {
            val key = ds.readAny() as Long;
	    val keyType = ds.readAny() as Int;
            val value = ds.readAny() as T;
            if (_debug_level > 5n) {
	        Console.OUT.println("[" + here.id + "] putForMove key: " + key + " keyType: " + keyType + " value: " + value);
            }
            toBranch.putForMove(key, keyType, value); 
        };
        mm.request(dest, serialize, deserialize);
    }

    public def moveAtSync(keys: List[Long], dest: Place, mm: MoveManagerLocal) throws Exception {
        if (dest == here) return;
	val collection = this;
	val serialize = (s: Serializer) => {
	    val size = keys.size();
	    s.writeAny(size);
	    for (key in keys) {
	        val value = collection.removeForMove(key);
		val keyType = ldist.moveOut(key, dest);
		s.writeAny(key);
		s.writeAny(keyType);
		s.writeAny(value);
	    }
	};
	val deserialize = (ds: Deserializer) => {
	    val size = ds.readAny() as Long;
	    for (i in 1..size) {
	        val key = ds.readAny() as Long;
		val keyType = ds.readAny() as Int;
		val value = ds.readAny() as T;
		collection.putForMove(key, keyType, value);
	    }
	};
	mm.request(dest, serialize, deserialize);
    }

    public def moveAtSyncCount(count: Long, dest: Place, mm: MoveManagerLocal) throws Exception {
        if (dest == here) return;
	val collection = this;
	val serialize = (s: Serializer) => {
	    val size = count;
	    s.writeAny(size);
            val keys = new Rail[Long](size);
            val values = new Rail[T](size);

            var i: Long = 0;
            for (entry in data.entries()) {
                if (i == size) break;
                keys(i) = entry.getKey();
                values(i) = entry.getValue();
                i += 1;
            }
            for (j in 0..(size - 1)) {
                s.writeAny(keys(j));
            }
            for (j in 0..(size - 1)) {
                s.writeAny(values(j));
            }
            for (j in 0..(size - 1)) {
                val key = keys(j);
                collection.removeForMove(key);
                val keyType = ldist.moveOut(key, dest);
                s.writeAny(keyType);
            }
	};
	val deserialize = (ds: Deserializer) => {
	    val size = ds.readAny() as Long;
            val keys = new Rail[Long](size);
            val values = new Rail[T](size);

            for (i in 0..(size - 1)) {
                keys(i) = ds.readAny() as Long;
            }
            for (i in 0..(size - 1)) {
                values(i) = ds.readAny() as T;
            }
	    for (i in 0..(size - 1)) {
                val keyType = ds.readAny() as Int;
		collection.putForMove(keys(i), keyType, values(i));
	    }
	};
	mm.request(dest, serialize, deserialize);
    }

    public def moveAtSync(rule:(Long)=>Place, mm:MoveManagerLocal) throws Exception {
        val collection = this;
	val keysToMove = new HashMap[Place, ArrayList[Long]]();
	collection.each((key: Long, value: T) => {
	    val destination = rule(key);
	    if (!keysToMove.containsKey(destination)) {
	        keysToMove(destination) = new ArrayList[Long]();
	    }
	    keysToMove(destination).add(key);
	});
	val places = keysToMove.keySet();
	for (place in places) {
	    moveAtSync(keysToMove(place), place, mm);
	}
    }

    public def moveAtSync(dist:Distribution[Long], mm:MoveManagerLocal) throws Exception {
        moveAtSync((key: Long) => dist.place(key), mm);
    }

    /* will be implemented in Java using TreeMap
    public def moveAtSync(range: LongRange, place: Place, mm:MoveManagerLocal) {U haszero}: void {
    
    }
    */
    // TODO???
    public def moveAtSync(dist:Distribution[LongRange], mm:MoveManagerLocal): void {
	// no need for sparse array
    }


    /**
     * Update the distribution information of the entries.
     */
    public def updateDist() throws Exception {
	ldist.updateDist(placeGroup, team);
    }

    /* Ensure calling updateDist() before balance()
     * balance() should be called in all places
     */
    public def balance(mm: MoveManagerLocal) throws Exception {
        val pgSize = placeGroup.size();
        val listPlaceLocality = new ArrayList[Pair[Long, Float]]();
        var localitySum: Float = 0.0f;
        var globalDataSize: Long = 0;
        val localDataSize = new Rail[Long](pgSize);

        for (val i in locality.range()) {
            localitySum += locality(i);
        }
        for (entry in ldist.dist.entries()) {
            //val k = entry.getKey();
            val v = entry.getValue();
            localDataSize(placeGroup.indexOf(v)) += 1; 
        }

        for (val i in 0..(pgSize - 1)) {
            globalDataSize += localDataSize(i);
            val normalizeLocality = locality(i) / localitySum;
            listPlaceLocality.add(Pair[Long,Float](i, normalizeLocality));
        }
        listPlaceLocality.sort((a1: Pair[Long, Float], a2: Pair[Long, Float]) => (a1.second < a2.second) ? -1n : (a1.second > a1.second) ? 1n : 0n);

        if (_debug_level > 5n) {
            for (pair in listPlaceLocality) {
                Console.OUT.print("(" + pair.first + ", " + pair.second + ") ");
            }
            Console.OUT.println();
            team.barrier(); // for debug print
        }

        val cumulativeLocality = new Rail[Pair[Long, Float]](pgSize);
        var sumLocality: Float = 0.0f;
        for (val i in 0..(pgSize - 1)) {
            sumLocality += listPlaceLocality(i).second;
            cumulativeLocality(i) = Pair[Long, Float](listPlaceLocality(i).first, sumLocality);
        }
        cumulativeLocality(pgSize - 1) = Pair[Long, Float](listPlaceLocality(pgSize - 1).first, 1.0f);

        if (_debug_level > 5n) {
            for (val i in 0..(pgSize - 1)) {
                val pair = cumulativeLocality(i);
                Console.OUT.print("(" + pair.first + ", " + pair.second + ", " + localDataSize(pair.first) + "/" + globalDataSize + ") ");
            }
            Console.OUT.println();
            team.barrier(); // for debug print
        }

        val moveList = new Rail[ArrayList[Pair[Long, Long]]](pgSize); // ArrayList(index of dest Place, num data to export)
        val stagedData = new ArrayList[Pair[Long, Long]](); // ArrayList(index of src, num data to export)
        var previousCumuNumData :Long = 0;

        for (val i in 0..(pgSize - 1)) {
            moveList(i) = new ArrayList[Pair[Long, Long]]();
        }

        for (val i in 0..(pgSize - 1)) {
            val placeIdx = cumulativeLocality(i).first;
            val placeLocality = cumulativeLocality(i).second;
            val cumuNumData = (globalDataSize as Float * placeLocality) as Long;
            val targetNumData = cumuNumData - previousCumuNumData;
            if (localDataSize(placeIdx) > targetNumData) {
                stagedData.add(Pair[Long, Long](placeIdx, localDataSize(placeIdx) - targetNumData));
                if (_debug_level > 5n) {
                    Console.OUT.print("stage src: " + placeIdx + " num: " + (localDataSize(placeIdx) - targetNumData) + ", ");
                }
            }
            previousCumuNumData = cumuNumData;
        }
        if (_debug_level > 5n) {
            Console.OUT.println();
            team.barrier(); // for debug print
        }

        previousCumuNumData = 0;
        for (val i in 0..(pgSize - 1)) {
            val placeIdx = cumulativeLocality(i).first;
            val placeLocality = cumulativeLocality(i).second;
            val cumuNumData = (globalDataSize as Float * placeLocality) as Long;
            val targetNumData = cumuNumData - previousCumuNumData;
            if (targetNumData > localDataSize(placeIdx)) {
                var numToImport :Long = targetNumData - localDataSize(placeIdx);
                while (numToImport > 0) {
                    val pair = stagedData.removeFirst();
                    if (pair.second > numToImport) {
                        moveList(pair.first).add(Pair[Long, Long](placeIdx, numToImport));
                        stagedData.add(Pair[Long, Long](pair.first, pair.second - numToImport));
                        numToImport = 0;
                    } else {
                        moveList(pair.first).add(Pair[Long, Long](placeIdx, pair.second));
                        numToImport -= pair.second;
                    }
                }
            }
            previousCumuNumData = cumuNumData;
        }

        if (_debug_level > 5n) {
            for (val i in 0..(pgSize - 1)) {
                for (pair in moveList(i)) {
                    Console.OUT.print("src: " + i + " dest: " + pair.first + " size: " + pair.second + ", ");
                }
            }
            Console.OUT.println();
            team.barrier(); // for debug print
        }


        if (_debug_level > 5n) {
            val diffNumData = new Rail[Long](pgSize, 0);
            for (val i in 0..(pgSize - 1)) {
                for (pair in moveList(i)) {
                    diffNumData(i) -= pair.second;
                    diffNumData(pair.first) += pair.second;
                }
            }
            for (pair in listPlaceLocality) {
                Console.OUT.print("(" + pair.first + ", " + pair.second + ", " + (localDataSize(pair.first) + diffNumData(pair.first)) + "/" + globalDataSize + ") ");
            }
            Console.OUT.println();
            team.barrier(); // for debug print
        }

        // Move Data
        for (val i in 0..(pgSize - 1)) {
            if (placeGroup(i) == here) {
                for (pair in moveList(i)) {
                    if (_debug_level > 5n) {
                        Console.OUT.println("MOVE src: " + i + " dest: " + pair.first + " size: " + pair.second);
                    }
                    moveAtSyncCount(pair.second, placeGroup(pair.first), mm);
                }
            }
        }

    }

    public def balance(newLocality: Rail[Float], mm: MoveManagerLocal) throws Exception {
        Rail.copy[Float](newLocality, locality);
        balance(mm);
    }


    static class DistIdMapLocal[V](ldist: DistManager.Index, locality: Rail[Float]) {V haszero} extends Local[Map[Long, V]] {

        def this(placeGroup: PlaceGroup, team: Team, data: Map[Long, V], ldist: DistManager.Index, locality: Rail[Float]) {
            super(placeGroup, team, data);
            property(ldist, locality);
        }
    }

}
