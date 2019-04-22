
package cassia.util;
import x10.util.*;
import x10.io.CustomSerialization;
import x10.io.Serializer;
import x10.io.Deserializer;

public class Chunk[T] extends AbstractCollection[T] implements /* RangedIndexed[T], RangedSettable[T],*/ RangedList[T], CustomSerialization {

    //    private val a: GrowableRail[T];
    private val a: Rail[T];
    public val range:LongRange;

    public static def make[T](c: Container[T], range:LongRange, v:T) {
	val a = new Chunk[T](range,v);
	a.addAll(c);
	return a;
    }
    public static def make[T](c: Container[T], range:LongRange) {T haszero} {
	val a = new Chunk[T](range);
	a.addAll(c);
	return a;
    }
    public def getRange() = range;
    public def contains(v: T): Boolean {
	/*        for (i in 0..(a.size()-1)) {
            if (v == null ? a(i) == null : v.equals(a(i))) {
                return true;
            }
	    }*/
	for (elem in a) {
            if (v == null ? elem == null : v.equals(elem)) {
                return true;
            }
	}
        return false;
    }
    
    public def clear(): void {
	throw new UnsupportedOperationException();
        // a.clear();
    }

    public def clone() {
	val aClone = new Rail[T](a);
	Rail.copy(a, aClone);
	return new Chunk[T](this.range, aClone);
    }

    public def cloneRange(newRange: LongRange): RangedList[T] {
        if (range == newRange) {
	    return clone();
	}
	return toChunk(newRange);
    }

    public def toChunk(newRange: LongRange): Chunk[T] {
        if ((newRange.min < range.min || range.max < newRange.min) ||
	    (newRange.max < range.min || range.max < newRange.max) ||
	    (newRange.min > newRange.max)) throw new ArrayIndexOutOfBoundsException();
	if (newRange == range) return this;
//	if (newRange.min == newRange.max) {
//	    return new Chunk[T](newRange, null);
//	}
	val newSize = newRange.max - newRange.min + 1;
	val newRail = new Rail[T](newSize, a(0));

	Console.OUT.println("range: " + range);
	Console.OUT.println("newRange: " + newRange);
	Console.OUT.println("newSize: " + newSize);
	Console.OUT.println("a: " + a);
	Console.OUT.println("chunk: " + this);

	Rail.copy(a, newRange.min - range.min, newRail, 0, newSize);
	return new Chunk[T](newRange, newRail);
    }

    public def splitRange(splitPoint: Long): Pair[RangedList[T], RangedList[T]] {
        val rangedList1 = new RangedListView[T](this, range.min..(splitPoint - 1));
	val rangedList2 = new RangedListView[T](this, splitPoint..range.max);
	return Pair[RangedList[T], RangedList[T]](rangedList1, rangedList2);
    }

    public def splitRange(splitPoint1: Long, splitPoint2: Long): Triplets[RangedList[T], RangedList[T], RangedList[T]] {
        val rangedList1 = new RangedListView[T](this, range.min..(splitPoint1 - 1));
	val rangedList2 = new RangedListView[T](this, splitPoint1..(splitPoint2 - 1));
	val rangedList3 = new RangedListView[T](this, splitPoint2..range.max);
	return Triplets[RangedList[T], RangedList[T], RangedList[T]](rangedList1, rangedList2, rangedList3);
    }

    public def last() {
	//	return range.min + a.size()-1;
	return range.max;
    }

    public def add(v: T): Boolean {
	throw new UnsupportedOperationException();
	/*	assert(a.size()+1 <= range.max-range.min+1);
        a.add(v);
        return true;*/
    }

    public def addAll(elems:Rail[T]):Boolean {
	throw new UnsupportedOperationException();
	/*	assert(a.size()+elems.size <= range.max-range.min+1);
        a.addAll(elems);
        return true;*/
    }
    public def remove(v: T): Boolean {
	throw new UnsupportedOperationException();
    }
    public def removeFirst(): T {
	throw new UnsupportedOperationException();
    }
    
    public operator this(i: Long)=(v: T) : T = set(v,i);
    
    public def set(v: T, i0: Long): T {
	val i = i0-range.min;
        a(i) = v;
        return v;
    }
    public def subList(fromIndex:Long, toIndex:Long): RangedList[T] {
    	if ((fromIndex < range.min || range.max < fromIndex) ||
	    (toIndex < range.min || range.max < toIndex) ||
	    (fromIndex > toIndex)) throw new ArrayIndexOutOfBoundsException();
	if (fromIndex == range.min && toIndex == range.max) return this;
	return new RangedListView[T](this, fromIndex..toIndex);
    }
    public def addBefore(i0: Long, v: T) {
	throw new UnsupportedOperationException();
    }
    public def removeAt(i0: Long): T {
	throw new UnsupportedOperationException();
	/*	val i = i0-range.min;
        val v = a(i);
        for (var j: Long = i+1; j < a.size(); j++) {
            a(j-1) = a(j);
        }
        a.removeLast();
        return v;*/
    }

    public operator this(i: Long): T = get(i);

    public def get(i0: Long): T {
	val i = i0-range.min;
	return a(i);
    }

    public def size(): Long = range.max-range.min+1;//a.size();
    
    public def isEmpty(): Boolean = a.size==0; //size() == 0;

    public def toRail() = a;//a.toRail();

    public def this(range: LongRange) {T haszero} {
        a = new Rail[T](range.max-range.min+1);
	this.range = range;
    }
    public def this(range: LongRange, v:T)  {
        a = new Rail[T](range.max-range.min+1, v);
	this.range = range;
    }
    public def this() {
        a = new Rail[T]();
	this.range = 0..0;
    }

    def this(range: LongRange, a:Rail[T]) {
	this.range = range;
	this.a = a;
    }


    public def removeLast(): T { throw new UnsupportedOperationException();} // = a.removeLast();
    public def reverse() { throw new UnsupportedOperationException();} 
    public def sort() {T <: Comparable[T]} { throw new UnsupportedOperationException();} 
    public def sort(cmp:(T,T)=>Int) { throw new UnsupportedOperationException();} 
    public def getFirst(): T = get(range.min);
    public def getLast(): T = get(range.max);

    public def indices(): List[Long] {
        val l = new ArrayList[Long]();
        for (i in range) {
            l.add(i);
        }
        return l;
    }
    
    public def indexOf(v: T): Long {
        return indexOf(range.min, v);
    }
    
    public def indexOf(index: Long, v: T): Long {
        for (var i: Long = index-range.min; i < a.size; i++) {
            if (v==null ? a(i)==null : v.equals(a(i)))
            	return i+range.min;
        }
        return -1;
    }
    
    public def lastIndexOf(v: T): Long {
        return lastIndexOf(range.max, v);
    }
    
    public def lastIndexOf(index: Long, v: T): Long {
        for (var i: Long = index-range.min; i >= 0; i--) {
            if (v==null ? a(i)==null : v.equals(a(i)))
            	return i+range.min;
        }
        return -1;
    }

    //
    // iterator
    //

// BIZARRE BUG: renaming S to T causes compiler to fail at isImplicitCastValid at end of X10MethodInstance_c.instantiate
    private static class It[S] implements ListIterator[S] {
        
        private var i: Long; // offset inside the chunk
        private val chunk: Chunk[S];
        
        def this(chunk: Chunk[S]) {
	    this.chunk = chunk;
            this.i = -1;
        }

        def this(chunk: Chunk[S], i0: Long) {
            this.chunk = chunk;
            this.i = i0-chunk.range.min-1;
        }
        
        public def hasNext(): Boolean {
            return i+1 < chunk.size();
        }

        public def nextIndex(): Long {
            return (++i) + chunk.range.min;
        }
        
        public def next(): S {
            return chunk.a(++i);
        }

        public def hasPrevious(): Boolean {
            return i-1 >= 0;
        }

        public def previousIndex(): Long {
            return (--i) + chunk.range.min;
        }
        
        public def previous(): S {
            return chunk.a(--i);
        }
        
        public def remove(): void {
	    chunk.removeAt(i + chunk.range.min);
        }
        
        public def set(v: S): void {
            chunk.a(i) = v;
        }
        
        public def add(v: S): void {
	    if(hasNext()) throw new UnsupportedOperationException("Iterator of Chunk only support add at last.");
            chunk.add(v);
        }
    }

    public def iterator(): ListIterator[T] {
        return new It[T](this);
    }
    
    public def iteratorFrom(i: Long): ListIterator[T] {
        return new It[T](this, i);
    }

    public def each(op: (T)=>void) {
	each(this.range, op);
    }
    public def each(range:LongRange, op: (T)=>void) {
	if (range.min < this.range.min || this.range.max < range.min ||
	    range.max < this.range.min || this.range.max < range.max ||
	    range.max < range.min) throw new ArrayIndexOutOfBoundsException();
	val from = Math.max(range.min, this.range.min);
	val to = Math.min(range.max, this.range.max);
	for (val i in from..to) op(get(i));
    }

    public def each[U](op: (T,Receiver[U])=>void, receiver: Receiver[U]) {
	each[U](this.range, op, receiver);
    }
    public def each[U](range:LongRange, op: (T,Receiver[U])=>void, receiver: Receiver[U]) {
	if (range.min < this.range.min || this.range.max < range.min ||
	    range.max < this.range.min || this.range.max < range.max ||
	    range.max < range.min) throw new ArrayIndexOutOfBoundsException();
	val from = Math.max(range.min, this.range.min);
	val to = Math.min(range.max, this.range.max);
	for (val i in from..to) op(get(i), receiver);
    }
    
    // public def sort(lessThan: (T,T)=>Boolean) = qsort(a, 0, a.size()-1, (x:T,y:T) => lessThan(x,y) ? -1 : (lessThan(y,x) ? 1 : 0));

    private def this(ds: Deserializer) {
        this.range = ds.readAny() as LongRange;
        this.a = ds.readAny() as Rail[T];
    }

    public def serialize(s:Serializer) {
//        Console.OUT.println("[" + here.id + "] serialize Chunk " + range);
	s.writeAny(range);
	s.writeAny(a);
    }

    /**
     * Return the string representation of this Chunk.
     * 
     * @return the string representation of this Chunk.
     */
    public def toString(): String {
        val sb = new x10.util.StringBuilder();
        sb.add("["+range+":");
        val sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
        for (var i:Long = 0; i < sz; ++i) {
            if (i > 0) sb.add(",");
            sb.add("" + a(i));
        }
        if (sz < size()) sb.add("...(omitted " + (size() - sz) + " elements)");
        sb.add("@" + range.min + ".."+last()+"]");
        return sb.toString();
    }

    public static def main(args:Rail[String]) {
	val i = 5;
	val c = new Chunk[Long]((10*i)..(11*i-1));
	Console.OUT.println("prepare:"+c);
	for(j in 0..(i-1)) {
	    Console.OUT.println("set@"+(10*i+j));
	    c(10*i+j) = 100*i+10*j;
	}
	Console.OUT.println(":"+ c);
    }
 }
