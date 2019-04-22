package cassia.dist;


import x10.io.Serializer;
import x10.io.Deserializer;
import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;
import x10.util.*;


/**
 * A class for handling objects using the Master-Proxy mechanism.
 * The master place has the body of each elements.
 * The proxy places have the branch of each elements.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o The first place of the PlaceGroup is selected as the master place automatically.
 * o To add any new elements is not allowed. The elements are assigned
 *   only in the construction.
 */
public class CacheableArray[T] {T haszero} implements List[T] {

    val localHandle: PlaceLocalHandle[CacheableArrayLocal[T]];

    private final def getLocalInternal(): CacheableArrayLocal[T] {
        return localHandle();
    }

    @TransientInitExpr(getSizeInternal())
    transient val size: Long;
    private final def getSizeInternal(): Long {
        val local = getLocalInternal();
        if (local == null) {
            return Zero.get[Long]();
        } else {
            return local.data.size();
        }
    }

    @TransientInitExpr(getDataInternal())
    transient val data: List[T];
    private final def getDataInternal(): List[T] {
        val local = getLocalInternal();
        if (local == null) {
            return new ArrayList[T]();
        } else {
            return local.data;
        }
    }

    @TransientInitExpr(getPlaceGroupInternal())
    transient val placeGroup: PlaceGroup;
    private final def getPlaceGroupInternal(): PlaceGroup {
        val local = getLocalInternal();
        if (local == null) {
            return Zero.get[PlaceGroup]();
        } else {
            return local.placeGroup;
        }
    }

    @TransientInitExpr(getTeamInternal())
    transient val team: Team;
    private final def getTeamInternal(): Team {
        val local = getLocalInternal();
        if (local == null) {
            return Zero.get[Team]();
        } else {
            return local.team;
        }
    }


    // Holds the master place of each element.
    // The elements are identified by Long values.
    // The Long values is for internal use so user cannot access them.
    @TransientInitExpr(getMasterMapInternal())
    transient val masterMap: Rail[Place];
    private final def getMasterMapInternal(): Rail[Place] {
        val local = getLocalInternal();
        if (local == null) {
            return new Rail[Place]();
        } else {
            return local.masterMap;
        }
    }

    /**
     * Create a new CacheableArray using the given list.
     * Place.places() is used as the PlaceGroup.
     *
     * @param indexed an instance of Indexed that is used for initializing the elements.
     */
    public def this(indexed: Indexed[T]) {
        this(Place.places(), Team.WORLD, indexed);
    }

    /**
     * Create a new CacheableArray using the given arguments.
     * The elements of new CacheableArray and given collection is the same.
     * The proxies are also set in the construction.
     *
     * @param placeGroup a PlaceGroup.
     * @param team a Team.
     * @param indexed an instance of Indexed that is used for initializing the elements.
     */
    public def this(placeGroup: PlaceGroup, team: Team, indexed: Indexed[T]) {
        this(placeGroup, team, placeGroup(0), indexed);
    }

    def this(placeGroup: PlaceGroup, team: Team, master: Place, indexed: Indexed[T]) {
        this(placeGroup, indexed, (): CacheableArrayLocal[T] => {
            val tmpData = new ArrayList[T](indexed.size());
            for (value in indexed) {
                tmpData.add(value);
            }
            val tmpMasterMap = new Rail[Place](tmpData.size());
            for (var i: Long = 0; i < tmpMasterMap.size; i++) {
                tmpMasterMap(i) = master;
            }
            return new CacheableArrayLocal[T](placeGroup, team, tmpData, tmpMasterMap);
        });
    }

    def this(placeGroup: PlaceGroup, indexed: Indexed[T], init: ()=>CacheableArrayLocal[T]) {
        this.localHandle = PlaceLocalHandle.makeFlat[CacheableArrayLocal[T]](placeGroup, init);
        this.size = getSizeInternal();
        this.data = getDataInternal();
        this.placeGroup = getPlaceGroupInternal();
        this.team = getTeamInternal();
        this.masterMap = getMasterMapInternal();
        for (var i: Long = 0; i < indexed.size(); i++) {
            data(i) = indexed(i);
        }
    }

    /**
     * Return the PlaceGroup.
     */
    public def placeGroup(): PlaceGroup = this.placeGroup;

    /**
     * Return the number of elements at local place.
     */
    public def size(): Long {
        return size;
    }

    /**
     * Return the iterator of local elements.
     */
    public def iterator(): ListIterator[T] {
        return this.data.iterator();
    }

    /**
     * Return a value corresponding to the given index.
     *
     * @return a value of T.
     */
    public operator this(i: Long): T {
        return data(i);
    }

    public def isEmpty(): Boolean {
        return data.isEmpty();
    }

    public def contains(v: T): Boolean {
        return data.contains(v);
    }

    public def containsAll(container: Container[T]): Boolean {
        return data.containsAll(container);
    }

    // return the elements as List
    // for internal use?
    /**
     * Return the all local elements as a List.
     *
     * @return the list of the local elements
     */
    public def toList(): List[T] {
        val list = new ArrayList[T](this.size());
        list.addAll(this.data);
        return list;
    }

    public def clone(): Collection[T] {
        return toList();
    }

    /**
     * Broadcast from master place to proxy place.
     * Packing elements using the specified function.
     * It is assumed that type U is declared as struct and it has no references.
     *
     * Note: Now, this method is implemented in too simple way.
     *
     * @param team a Team used in broadcast the packed data.
     * @param pack a function which packs the elements of master node.
     * @param unpack a function which unpacks the received data and inserts the unpacked data to each proxy.
     */
    public def broadcast[U](pack: (T)=>U, unpack: (T,U)=>void) {U haszero}: void {
	val serProcess = (ser:Serializer)=> {
	    for(elem in data) {
		ser.writeAny(pack(elem));
	    }
	};
	val desProcess = (des:Deserializer)=> {
	    for(elem in data) {
		unpack(elem, des.readAny() as U);
	    }
	};
	CollectiveRelocator.bcastSer(placeGroup, team, placeGroup(0), serProcess, desProcess);
    }

    public def indices():List[Long] = this.data.indices();
    public def reverse() {
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
    public def removeAt(i0: Long): T {
	throw new UnsupportedOperationException();
    }

    public def removeFirst(): T {
	throw new UnsupportedOperationException();
    }
    public def removeLast(): T {
	throw new UnsupportedOperationException();
    }
    public def indexOf(v: T): Long = this.data.indexOf(v);
    public def lastIndexOf(v: T): Long = this.data.lastIndexOf(v);
    public def indexOf(index: Long, v: T): Long = this.data.indexOf(index,v);
    public def lastIndexOf(index: Long, v: T): Long = this.data.lastIndexOf(index,v);
    public def iteratorFrom(index:Long): ListIterator[T] = this.data.iteratorFrom(index);
    public def subList(fromIndex:Long, toIndex:Long): List[T] {
	throw new UnsupportedOperationException();
    }
    public def getFirst(): T  = this.data.getFirst();
    public def getLast(): T = this.data.getLast();
    public def sort() { throw new UnsupportedOperationException();} 
    public def sort(cmp:(T,T)=>Int) { throw new UnsupportedOperationException();} 
    public def add(v: T): Boolean {
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
    public def clear(): void {
	throw new UnsupportedOperationException();
    }
    public operator this(i: Long)=(v: T) : T = set(v,i);
    
    public def set(v: T, i0: Long): T {
     	throw new UnsupportedOperationException();
    }

    public def toString(): String {
        val sb = new StringBuilder();
        val ei = this.data.iterator();
        sb.add("CacheableArray[");
        while (true) {
            if (ei.hasNext()) {
                sb.add(ei.next());
            } else {
                break;
            }
            if (ei.hasNext()) {
                sb.add(" ");
            }
        }
        sb.add("]");
        return sb.result();
    }


    // For internal use.
    // This class is only used as local storage of masterMap.
    private final static class CacheableArrayLocal[S](masterMap: Rail[Place]) extends Local[List[S]] {

        def this(placeGroup: PlaceGroup, team: Team, data: List[S], masterMap: Rail[Place]) {
            super(placeGroup, team, data);
            property(masterMap);
        }
    }
}

