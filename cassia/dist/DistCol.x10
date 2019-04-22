package cassia.dist;

import cassia.concurrent.Pool;
import x10.compiler.Inline;
import x10.compiler.TransientInitExpr;
import x10.util.concurrent.Condition;
import x10.util.*;
import x10.io.Serializer;
import x10.io.Deserializer;
import cassia.util.*;

/**
 * A class for handling objects at multiple places.
 * It is allowed to add new elements dynamically.
 * This class provides the method for load balancing.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o There is only one load balancing method.
 *   The method flattens the number of elements of the all places.
 */
public class DistCol[T] {T haszero} extends AbstractDistCollection[ChunkedList[T]] implements List[T], ManagedDistribution[LongRange] {

    private static val _debug_level :Int = 5n;

    private final def getLocalInternal(): DistColLocal[T] {
        return getLocal[DistColLocal[T]]();
    }

    @TransientInitExpr(getLocalLDist())
    transient val ldist: DistManager.Range;
    private final def getLocalLDist(): DistManager.Range {
        val local = getLocalInternal();
        if (local == null) {
            return new DistManager.Range();
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
    public def getRangedDistributionLong() { return new RangedDistributionLong(getDist()); }
    public def getDistributionLong() { return new DistributionLong(getDist()); }

    var proxy:(Long)=>T = null;
    public def setProxy(proxy:(Long)=>T) {
	this.proxy = proxy;
    }

    public operator this(i: Long): T {
	if(proxy==null) {
	    return getLocalInternal().data(i);
	} else {
	    try {
		return getLocalInternal().data(i);
	    } catch (e: IndexOutOfBoundsException) {
		return proxy(i);
	    }
	}
    }
    /*
    public def containIndex(i: Long): Boolean {
        return getLocalInternal().data.containIndex(i);
    }
    */

    public operator this(i: Long) = (value: T): T {
        return getLocalInternal().data(i) = value;
    }

    public def size(): Long = getLocalInternal().data.size();
    public def isEmpty(): Boolean = getLocalInternal().data.isEmpty();
    public def contains(v:T): Boolean = getLocalInternal().data.contains(v);
    public def containsAll(vs:Container[T]): Boolean = getLocalInternal().data.containsAll(vs);
    public def clone(): DistCol[T] {
	throw new UnsupportedOperationException("DistCol does not support clone because it is missleading.");
    }
    public def iterator(): ListIterator[T] = getLocalInternal().data.iterator();

    public def clear() { 
        getLocalInternal().data.clear();
	ldist.clear();
        this.locality.fill(1.0f);
    }

    public def putChunk(c: RangedList[T]) throws Exception {
	ldist.add(c.getRange());
	getLocalInternal().data.addChunk(c);
    }

    private def putForMove(c: RangedList[T], keyType: Int) throws Exception {
        val key = c.getRange();
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
	getLocalInternal().data.addChunk(c);
    }

    public def removeChunk(c: RangedList[T]) throws Exception {
	ldist.remove(c.getRange());
	getLocalInternal().data.removeChunk(c); 
    }

    private def removeForMove(c: RangedList[T]) throws Exception {
	if (!getLocalInternal().data.removeChunk(c)) {
	    throw new Exception("DistCol#removeForMove");
	} 
    }

    public def integrate(src : ChunkedList[T]) {
	//TODO
        throw new UnsupportedOperationException();
    }

    static struct ChunkExtractLeft[T] {
        public val original: RangedList[T]; 
        public val splitPoint: Long;
	def this(original: RangedList[T], splitPoint: Long) {
	    this.original = original;
	    this.splitPoint = splitPoint;
	}
	def extract() {
	    return original.splitRange(splitPoint);
	}
    }

    static struct ChunkExtractMiddle[T] {
        public val original: RangedList[T]; 
        public val splitPoint1: Long;
        public val splitPoint2: Long;
	def this(original: RangedList[T], splitPoint1: Long, splitPoint2: Long) {
	    this.original = original;
	    this.splitPoint1 = splitPoint1;
	    this.splitPoint2 = splitPoint2;
	}
	def extract() {
	    return original.splitRange(splitPoint1, splitPoint2);
	}
    }

    static struct ChunkExtractRight[T] {
        public val original: RangedList[T]; 
        public val splitPoint: Long;
	def this(original: RangedList[T], splitPoint: Long) {
	    this.original = original;
	    this.splitPoint = splitPoint;
	}
	def extract() {
	    return original.splitRange(splitPoint);
	}
    }

    public def moveAtSync(range: LongRange, dest: Place, mm: MoveManagerLocal) throws Exception {
        if (_debug_level > 5n) {
            Console.OUT.println("[" + here.id + "] moveAtSync range: " + range + " dest: " + dest.id);
        }
        val chunksToMove = new ArrayList[RangedList[T]]();
        val chunksToExtractLeft = new ArrayList[ChunkExtractLeft[T]]();
        val chunksToExtractMiddle = new ArrayList[ChunkExtractMiddle[T]]();
        val chunksToExtractRight = new ArrayList[ChunkExtractRight[T]]();
        getLocalInternal().data.eachChunk((c: RangedList[T]) => {
	    val cRange = c.getRange();
	    if (cRange.min <= range.min) {
	        if (cRange.max < range.min) {
		    // skip
		} else {
		    // range.min <= cRange.max
		    if (cRange.min == range.min) {
		        if (cRange.max <= range.max) {
			    // add cRange.min..cRange.max
			    chunksToMove.add(c);
 			} else {
			    // range.max < cRange.max
			    // split at range.max/range.max+1
			    // add cRange.min..range.max
			    chunksToExtractLeft.add(ChunkExtractLeft[T](c, range.max + 1));
			}
		    } else {
		        // cRange.min < range.min
			if (range.max < cRange.max) {
			    // split at range.min-1/range.min
			    // split at range.max/range.max+1
			    // add range.min..range.max
			    chunksToExtractMiddle.add(ChunkExtractMiddle[T](c, range.min, range.max + 1));
			} else {
			    // split at range.min-1/range.min
			    // cRange.max =< range.max
			    // add range.min..cRange.max
			    chunksToExtractRight.add(ChunkExtractRight[T](c, range.min));
			}
		    }
		}
	    } else {
	        // range.min < cRange.min
		if (range.max < cRange.min) {
		    // skip
		} else {
		    // cRange.min <= range.max
		    if (cRange.max <= range.max) {
		        // add cRange.min..cRange.max
			chunksToMove.add(c);
		    } else {
		        // split at range.max/range.max+1
			// add cRange.min..range.max
			chunksToExtractLeft.add(ChunkExtractLeft[T](c, range.max + 1));
		    }
		}
	    }
	});

	for (chunkToExtractLeft in chunksToExtractLeft) {
	    val original = chunkToExtractLeft.original;
	    val splits = chunkToExtractLeft.extract();
//	    Console.OUT.println("[" + here.id + "] removeChunk " + original.getRange());
	    removeChunk(original);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.first.getRange());
	    putChunk(splits.first);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.second.getRange());
	    putChunk(splits.second);
	    chunksToMove.add(splits.first);
	}

	for (chunkToExtractMiddle in chunksToExtractMiddle) {
	    val original = chunkToExtractMiddle.original;
	    val splits = chunkToExtractMiddle.extract();
//	    Console.OUT.println("[" + here.id + "] removeChunk " + original.getRange());
	    removeChunk(original);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.first.getRange());
	    putChunk(splits.first);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.second.getRange());
	    putChunk(splits.second);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.third.getRange());
	    putChunk(splits.third);
	    chunksToMove.add(splits.second);
	}

	for (chunkToExtractRight in chunksToExtractRight) {
	    val original = chunkToExtractRight.original;
	    val splits = chunkToExtractRight.extract();
//	    Console.OUT.println("[" + here.id + "] removeChunk " + original.getRange());
	    removeChunk(original);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.first.getRange());
	    putChunk(splits.first);
//	    Console.OUT.println("[" + here.id + "] putChunk " + splits.second.getRange());
	    putChunk(splits.second);
	    chunksToMove.add(splits.second);
	}

	moveAtSync(chunksToMove, dest, mm);
    }



    public def moveAtSync(cs: List[RangedList[T]], dest: Place, mm: MoveManagerLocal) throws Exception {
        if (_debug_level > 5n) {
            Console.OUT.print("[" + here.id + "] moveAtSync List[RangedList[T]]: ");
	    for (rl in cs) {
	        Console.OUT.print("" + rl.getRange() + ", ");
	    }
	    Console.OUT.println(" dest: " + dest.id);
        }

        if(dest == here) return;

	val toBranch = this; // using plh@AbstractCol
        val serialize = (s: Serializer) => {
	    val keyTypeList = new ArrayList[Int]();
	    for(c in cs) {
	        keyTypeList.add(ldist.moveOut(c.getRange(), dest));
		this.removeForMove(c);
	    }
	    s.writeAny(keyTypeList);
	    s.writeAny(cs);
        };
        val deserialize = (ds: Deserializer) => {
	    val keyTypeList = ds.readAny() as List[Int];
	    val keyTypeListIt = keyTypeList.iterator();
	    val chunks = ds.readAny() as List[RangedList[T]];
	    for(c in chunks) {
	        val keyType = keyTypeListIt.next();
		val key = c.getRange();
                if (_debug_level > 5n) {
	            Console.OUT.println("[" + here.id + "] putForMove key: " + key + " keyType: " + keyType);
                }
		toBranch.putForMove(c, keyType); 
	    }
        };
        mm.request(dest, serialize, deserialize);
    }

    public def moveAtSyncCount(moveList: ArrayList[Pair[Long, Long]], mm: MoveManagerLocal) throws Exception {
        val localKeys = new ArrayList[LongRange]();
        localKeys.addAll(ranges());
        localKeys.sort((range1: LongRange, range2: LongRange) => {
            val len1 = range1.max - range1.min;
            val len2 = range2.max - range2.min;
            return (len1 - len2) as Int;
        });
        if (_debug_level > 5n) {
            Console.OUT.print("[" + here.id + "] ");
            for (i in 0..(localKeys.size() - 1)) {
                Console.OUT.print("" + localKeys(i).min + ".." + localKeys(i).max + ", ");
            }
            Console.OUT.println();
        }
        for (moveinfo in moveList) {
            val count = moveinfo.second;
            val dest = placeGroup(moveinfo.first);
            if (_debug_level > 5n) {
                Console.OUT.println("[" + here.id + "] move count=" + count + " to dest " + dest.id);
            }
            if (dest == here) continue;
            var sizeToSend: Long = count;
            while (sizeToSend > 0) {
                val lk = localKeys.removeFirst();
                val len = lk.max - lk.min + 1;
                if (len > sizeToSend) {
                    moveAtSync(lk.min..(lk.min + sizeToSend - 1), dest, mm);
                    localKeys.addBefore(0N, (lk.min + sizeToSend)..lk.max);
                    break;
                } else {
                    moveAtSync(lk, dest, mm);
                    sizeToSend -= len;
                }
            }
        }
    }

    public def moveAtSync(rule: (LongRange)=>List[Pair[Place, LongRange]],
        mm: MoveManagerLocal) throws Exception {
	val collection = this;
	val rangesToMove = new HashMap[Place, ArrayList[LongRange]]();
	collection.eachChunk((c: RangedList[T]) => {
	    val destinationList = rule(c.getRange());
	    for (destination in destinationList) {
	        val destinationPlace = destination.first;
		val destinationRange = destination.second;
		if (!rangesToMove.containsKey(destinationPlace)) {
		    rangesToMove(destinationPlace) = new ArrayList[LongRange]();
		}
		rangesToMove(destinationPlace).add(destinationRange);
	    }
	});
	val places = rangesToMove.keySet();
	for (place in places) {
	    for (range in rangesToMove(place)) {
	        moveAtSync(range, place, mm);
	    }
	}
    }

    public def moveAtSync(dist: RangedDistribution[LongRange], mm: MoveManagerLocal) throws Exception {
        moveAtSync((range: LongRange) => dist.placeRanges(range), mm);
    }

    public def moveAtSync(dist: Distribution[Long], mm: MoveManagerLocal) throws Exception {
        moveAtSync((range: LongRange) => {
	    val listPlaceRange = new ArrayList[Pair[Place, LongRange]]();
	    for (key in range) {
	        listPlaceRange.add(Pair[Place, LongRange](dist.place(key), key..key));
	    }
	    return listPlaceRange;
	}, mm);
    }

/*
    public def moveAtSync(dist:RangedDistribution[LongRange], mm:MoveManagerLocal) {
	// moveAtSync(range, ..) を複数回呼ぶか
	// あるいは、moveAtSync(List[LongRange],) を作成して、それを一回呼ぶか。
	// いずれにせよ、closure の中身を private method に分離して、少し掃除したほうがいいかも？
	for(place in placeGroup) {
	    val ranges = dist.ranges(place);
	    for(range in ranges) moveAtSync(range, place, mm);
	}
    }
*/

/*
    public def relocate(dist:RangedDistribution) {
	val mm = new MoveManagerLocal(placeGroup,team);
	moveAtSync(dist, mm);
	mm.sync();
    }
*/

    public def updateDist() {
	ldist.updateDist(placeGroup,team);
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
            val k = entry.getKey();
            val v = entry.getValue();
            localDataSize(placeGroup.indexOf(v)) += k.max - k.min + 1; 
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


        for (val i in 0..(pgSize - 1)) {
            if (placeGroup(i) == here) {
                moveAtSyncCount(moveList(i), mm);
            }
        }
    }

    public def balance(newLocality: Rail[Float], mm: MoveManagerLocal) throws Exception {
        Rail.copy[Float](newLocality, locality);
        balance(mm);
    }

    public def balance(): void {
        // new LoadBalancer[T](data, placeGroup, team).execute();
        throw new UnsupportedOperationException();
    }

    //TODO
    public def ranges(): Container[LongRange] = getLocalInternal().data.ranges();

    // TODO
    public def each[U](pool: Pool, receiverHolder: ReceiverHolder[U], nth: Long, op: (T, Receiver[U])=>void): void {
        if (isEmpty()) {
            return;
        }
        ParallelAccumulator.execute(pool, getLocalInternal().data, receiverHolder, nth, op);
    }

    public def asyncEach[U](pool: Pool, receiverHolder: ReceiverHolder[U], nth: Long, op: (T, Receiver[U])=>void): Condition {
        if (isEmpty()) {
            val condition = new Condition();
            condition.release();
            return condition;
        }
        if (_debug_level > 5n) {
	    Console.OUT.println("DistCol#asyncEach@ " + here + " data:" + data.ranges());
        }
        return ParallelAccumulator.executeAsync(pool, getLocalInternal().data, receiverHolder, nth, op);
    }

    public def eachChunk(op: (RangedList[T])=>void): void {
        getLocalInternal().data.eachChunk(op);
    }

    public def filterChunk(op: (RangedList[T])=>boolean): List[RangedList[T]] {
        return getLocalInternal().data.filterChunk(op);
    }

    public def indices():List[Long] {
	throw new UnsupportedOperationException();
    }
    public def reverse() {
	throw new UnsupportedOperationException();
    }
    public def add(v: T): Boolean {
	throw new UnsupportedOperationException();
    }
    public def addBefore(i:Long, v: T) {
	throw new UnsupportedOperationException();
    }


    public def addAll(elems:Container[T]):Boolean {
	throw new UnsupportedOperationException();
    }
    public def remove(v: T): Boolean {
	throw new UnsupportedOperationException();
    }
    public def retainAll(vs: Container[T]):Boolean {
	throw new UnsupportedOperationException();
    }
    public def removeAll(vs: Container[T]):Boolean {
	throw new UnsupportedOperationException();
    }
    public def addAllWhere(c: Container[T], p:(T)=>Boolean):Boolean {
	throw new UnsupportedOperationException();
    }
    public def removeAllWhere(p:(T)=>Boolean):Boolean {
	throw new UnsupportedOperationException();
    }
    public def removeAt(i0: Long): T {
	throw new UnsupportedOperationException();
    }

    public def removeFirst(): T {
	throw new UnsupportedOperationException();
    }
    public def removeLast(): T {
	throw new UnsupportedOperationException();
    }
    public def indexOf(v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def lastIndexOf(v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def indexOf(index: Long, v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def lastIndexOf(index: Long, v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def iteratorFrom(index:Long): ListIterator[T] {
	throw new UnsupportedOperationException();
    }
    public def subList(fromIndex:Long, toIndex:Long): List[T] {
	throw new UnsupportedOperationException();
    }
    public def getFirst(): T  {
	throw new UnsupportedOperationException();
    }
    public def getLast(): T {
	throw new UnsupportedOperationException();
    }
    public def sort() { throw new UnsupportedOperationException();} 
    public def sort(cmp:(T,T)=>Int) { throw new UnsupportedOperationException();} 

    /**
     * Create a new DistCol using the given arguments.
     *
     * @param placeGroup an instance of PlaceGroup.
     * @param team an instance of Team.
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
        this(placeGroup, team, (): ChunkedList[T] => new ChunkedList[T]());
    }

    public def this(placeGroup: PlaceGroup, team:Team, 
	init: ()=>ChunkedList[T]) {
        this(placeGroup, (): Local[ChunkedList[T]] => {
	    val data = init();
	    val ldist = new DistManager.Range();
	    ldist.setup(data.ranges());
            val locality = new Rail[Float](placeGroup.size(), 1.0f);
	    return new DistColLocal[T](placeGroup, team, data, ldist, locality);
	});
    }

    def this(placeGroup:PlaceGroup, init:()=>Local[ChunkedList[T]]) {
	super(placeGroup, init);
	this.ldist = getLocalLDist();
        this.locality = getLocalLocality();
    }

    static class DistColLocal[S](ldist: DistManager.Range, locality: Rail[Float]) {S haszero} extends Local[ChunkedList[S]] {
        def this(placeGroup: PlaceGroup, team: Team, data: ChunkedList[S], ldist:DistManager.Range, locality: Rail[Float]) {
            super(placeGroup, team, new ChunkedList[S]());
	    property(ldist, locality);
        }
    }

}
