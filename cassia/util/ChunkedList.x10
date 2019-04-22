package cassia.util;

import cassia.concurrent.Pool;
import x10.compiler.Inline;
import x10.compiler.TransientInitExpr;
import x10.util.concurrent.Condition;
import x10.util.*;
import x10.io.Console;


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
public class ChunkedList[T] {T haszero} implements List[T] { //TODO read, write both??

    private val chunks: List[RangedList[T]];

    public def this() {
	chunks = new ArrayList[RangedList[T]]();
    }
    public def this(chunks: List[RangedList[T]]) {
	this.chunks = chunks;
    }

    // TODO too slow when #of chunks become large. Please introduce O(logN) algorithm.
    // (use TreeMap or something in Java)
    def checkBounds(index: Long): Long {
	var i:Long = 0;
        for (c in chunks) {
            val range = c.getRange();
            if (index >= range.min && index <= range.max) {
		return i;
            }
	    i++;
        }
	// TODO throw error or not.
        throw new IndexOutOfBoundsException("ChunkedList: index " + index + " is out of range of " + chunks);
    }
    // used in getReceiver
    def checkDuplicate(range: LongRange): void {
        for(c in chunks) {
            val existingRange = c.getRange();
            if (range.max < existingRange.min) {
                continue;
            }
            if (existingRange.max < range.min) {
                continue;
            }
	    // TODO throw error or not.
            throw new Exception("CheckedList#checkDUplicate: requested range " + range + " is duplicated with " + existingRange);
        }
    }
    /*
    public def containIndex(i: Long): Boolean {
	val r = checkBounds(i);
	return  (r>= 0);
    }
    */

    /**
     * Return the value correspond to the specified logical index.
     *
     * @return a value of T.
     */
    public operator this(i: Long): T = get(i);

    public def get(i: Long): T {
        val number = checkBounds(i);
        val chunk = chunks(number);
	val r = chunk(i);
	return r;
    }

    public operator this(i: Long) = (value: T): T = set(value,i);
    public def set(value: T, i: Long): T {
        val number = checkBounds(i);
        val chunk = chunks(number);
	return (chunk(i)=value);
    }

    public def isEmpty(): Boolean {
	for(chunk in chunks) {
	    if(!chunk.isEmpty()) return false;
	}
	return true;
    }

    /**
     * Clear the local elements.
     */
    public def clear(): void {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Return whether this chunked list contain the given chunk.
     */
    public def containsChunk(c:RangedList[T]): Boolean {
	return chunks.contains(c);
    }


    /**
     * Return whether this chined list contains the given value.
     * This method uses T#equals() to evaluate the equality.
     *
     * @param v a value of type T.
     * @return true or false.
     */
    public def contains(v: T): Boolean {
	for(chunk in chunks) {
	    if(chunk.contains(v)) return true;
	}
        return false;
    }

    public def containsAll(container: Container[T]): Boolean {
	for(v in container) {
	    if(!this.contains(v)) return false;
	}
        return true;
    }


    /**
     * Return the number of local elements.
     *
     * @return the number of local elements.
     */
    public def size(): Long {
	var result:Long = 0;
	for(chunk in chunks) {
	    result += chunk.size();
	}
	return result;
    }


    /**
     * Return a Container that has the same values in the DistCol.
     *
     * @return a Container that has the same values in the DistCol.
     */
    public def clone(): Collection[T] {
        val newChunks = new ArrayList[RangedList[T]]();
        for (c in chunks) {
	    newChunks.add(c.clone() as Chunk[T]);
	}
        return new ChunkedList[T](newChunks);
    }

    /**
     * Return the logical range assined to local.
     *
     * @return an instance of LongRange.
     */
    public def ranges(): Container[LongRange] {
	val ranges = new ArrayList[LongRange]();
	for(c in chunks) ranges.add(c.getRange());
        return ranges;
    }

    public def addChunk(c:RangedList[T]) {
	checkDuplicate(c.getRange());
	chunks.add(c);
    }

    public def removeChunk(c: RangedList[T]): Boolean {
        return chunks.remove(c);
    }

    public def each(op: (T)=>void) {
	for(c in chunks) c.each(op);
    }
    /*    public def each(range:LongRange, op: (T)=>void) {
	for(c in chunks) c.each(range, op);
	}*/

    public def each[U](op: (T,Receiver[U])=>void, receiver: Receiver[U]) {
	val op2 = (t:T)=>{ op(t,receiver); };
	for(c in chunks) c.each(op2);
    }
    /*    public def each[U](range:LongRange, op: (T,Receiver[U])=>void, receiverHolder: ReceiverHolder[U]) {
	for(c in chunks) c.each(range, op, receiverHolder);
	}*/

    public def eachChunk(op: (RangedList[T])=>void): void {
        for (c in chunks) {
	    op(c);
	}
    }

    public def filterChunk(op: (RangedList[T])=>boolean): List[RangedList[T]] {
        val list = new ArrayList[RangedList[T]]();
        for (c in chunks) {
	    if (op(c)) {
	        list.add(c);
	    }
	}
	return list;
    }

    public def separate(n:Long): Rail[ChunkedList[T]] {
	val totalNum = size();
	val rem = totalNum % n;
	val quo = totalNum / n;
	val result = new Rail[ChunkedList[T]](n);
	var chunkCount:Long = 0;
	var used:Long = 0;
	for(i in 0..(n-1)) {
	    val r = new ChunkedList[T]();
	    result(i) = r;
	    var rest:Long = quo + ((i<rem)? 1: 0);
	    while(rest > 0) {
		val c = chunks(chunkCount);
		if(c.size() - used <= rest) {
		    val from = c.getRange().min + used;
		    r.addChunk(c.subList(from, c.getRange().max));
		    rest -= c.size()-used;
		    used = 0;
		    chunkCount++;
		} else {
		    val from = c.getRange().min + used;
		    val to = from + rest;
		    r.addChunk(c.subList(from, to-1));
		    used += rest;
		    rest = 0;
		}
	    }
	}
	return result;
    }

    // TODO public def subList(range:LongRange): RangedList[T];

    /**
     * Return the iterator for the local elements.
     *
     * @return the iterator.
     */

    private static class It[S] (chunks:List[RangedList[S]])implements ListIterator[S] {
	private var ic: Long = -1;
	private var cIter: ListIterator[S];
	
        public def hasNext(): Boolean {
	    if(ic > chunks.size()) return false;
	    while (ic < 0 || (cIter.hasNext() == false)) {
		ic++;
		if(ic >= chunks.size()) return false;
		cIter = chunks(ic).iterator();
	    }
	    return true;
        }
        public def next(): S {
	    if(hasNext()) return cIter.next();
	    throw new IndexOutOfBoundsException();
        }
        public def nextIndex(): Long {
	    if(hasNext()) return cIter.nextIndex();
	    throw new IndexOutOfBoundsException();
        }

        public def hasPrevious(): Boolean {
	    throw new UnsupportedOperationException();
        }

        public def previousIndex(): Long {
	    throw new UnsupportedOperationException();
        }
        
        public def previous(): S {
	    throw new UnsupportedOperationException();
        }
        
        public def remove(): void {
	    throw new UnsupportedOperationException();
        }
        
        public def set(v: S): void {
	    throw new UnsupportedOperationException();
        }
        
        public def add(v: S): void {
	    throw new UnsupportedOperationException();
        }
    }

    public def iterator(): ListIterator[T] { //TODO
	return new It[T](chunks);
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
    public def indexOf(index: Long, v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def lastIndexOf(index: Long, v: T): Long {
	throw new UnsupportedOperationException();
    }
    public def lastIndexOf(v: T): Long {
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
     * Return the string representation of this Chunk.
     * 
     * @return the string representation of this Chunk.
     */
    public def toString(): String {
        val sb = new x10.util.StringBuilder();
        sb.add("[ChunksList("+chunks.size()+")");
        for (c in chunks) {
            sb.add("," + c);
        }
        sb.add("]");
        return sb.toString();
    }

    public static def main(args:Rail[String]) {

	val c0 = new Chunk[Long]((10*10)..(11*10-1));
	for(y in c0) {
	    Console.OUT.println(", "+y);
	}


	val clist = new ChunkedList[Long]();

	for(i in 1..5) {
	    val c = new Chunk[Long]((10*i)..(11*i-1));
	    Console.OUT.println("prepare:"+c);
	    for(j in 0..(i-1)) {
	    Console.OUT.println("set@"+(10*i+j));
		c(10*i+j) = 100*i+10*j;
	    }
	    clist.addChunk(c);
	}
	for(x in clist) Console.OUT.print(":"+x);
	Console.OUT.println(":done");
	/*	for(x in [10, 50, 48]) {
	    Console.OUT.println("clist("+x+"):"+(clist.containIndex(x)?clist(x):-100));
	    }*/




	for(num in [1,2,3,4,5,50]) {
	    val result = clist.separate(num);
	    for(r in result) {
		Console.OUT.println(":"+num+">" + r);
	    }
	}
	Console.OUT.println("body:");
	val iter = clist.iterator();
	var p:Long=0;
	while(iter.hasNext()) {
	    Console.OUT.println(""+(p++)+","+iter.next() +", ");
	}
    }
    

}
