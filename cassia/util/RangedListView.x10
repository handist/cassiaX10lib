package cassia.util;

import x10.util.*;
import x10.io.CustomSerialization;
import x10.io.Serializer;
import x10.io.Deserializer;

public class RangedListView[T] implements RangedList[T], CustomSerialization {

    val base: List[T];
    val range: LongRange;

    public static def emptyView[T]() =  new RangedListView[T](null, 0..-1);

    public def this(base: List[T], range: LongRange) {
        this.base = base;
	this.range = range;
    }

    public def getRange() = range;

    public def contains(v: T): Boolean {
	for (i in range) {
	    val elem = base(i);
            if (v == null ? elem == null : v.equals(elem)) {
                return true;
            }
	}
        return false;
    }
    public def containsAll(vs: Container[T]): Boolean {
	for(v in vs) if(!contains(v)) return false;
	return true;
    }
    public def indexOf(v: T): Long {
	return indexOf(range.min,v);
    }
    public def indexOf(index: Long, v: T): Long {
        for (i in range) {
            if (v==null ? base(i)==null : v.equals(base(i)))
            	return i;
        }
        return -1;
    }
    public def lastIndexOf(v: T): Long {
	return indexOf(range.max,v);
    }
    public def lastIndexOf(index: Long, v: T): Long {
        for (var i:Long=index; i>=range.min; i--) {
            if (v==null ? base(i)==null : v.equals(base(i)))
            	return i;
        }
        return -1;
    }

    
    public def clear(): void {
	throw new UnsupportedOperationException();
    }

    public def clone(): RangedList[T] {  // Is it should return Collection[T] ?
	return cloneRange(range);
    }

    public def cloneRange(newRange: LongRange): RangedList[T] {
	return toChunk(newRange);
    }

    public def toChunk(newRange: LongRange): Chunk[T] {
//        Console.OUT.println("range: " + range);
//        Console.OUT.println("newRange: " + newRange);
        if (newRange.min < range.min || range.max < newRange.min ||
	    newRange.max < range.min || range.max < newRange.max ||
	    newRange.min > newRange.max) {
	    throw new ArrayIndexOutOfBoundsException();
	}
//	if (newRange.min == newRange.max) {
//	    return new Chunk[T](newRange, null);
//	}
        if (base instanceof ArrayList[T]) {
	    val subArrayList = base.subList(newRange.min, newRange.max + 1) as ArrayList[T];
	    return new Chunk[T](newRange, subArrayList.toRail());
	} else if (base instanceof Chunk[T]) {
	    return (base as Chunk[T]).cloneRange(newRange) as Chunk[T];
	} else if (base instanceof RangedListView[T]) {
	    return (base as RangedListView[T]).toChunk(newRange);
	} else {
	    throw new UnsupportedOperationException();
	}
    }

    public def splitRange(splitPoint: Long): Pair[RangedList[T], RangedList[T]] {
        val rangedList1 = new RangedListView[T](base, range.min..(splitPoint - 1));
	val rangedList2 = new RangedListView[T](base, splitPoint..range.max);
	return Pair[RangedList[T], RangedList[T]](rangedList1, rangedList2);
    }

    public def splitRange(splitPoint1: Long, splitPoint2: Long): Triplets[RangedList[T], RangedList[T], RangedList[T]] {
        val rangedList1 = new RangedListView[T](base, range.min..(splitPoint1 - 1));
	val rangedList2 = new RangedListView[T](base, splitPoint1..(splitPoint2 - 1));
	val rangedList3 = new RangedListView[T](base, splitPoint2..range.max);
	return Triplets[RangedList[T], RangedList[T], RangedList[T]](rangedList1, rangedList2, rangedList3);
    }

    public def toRail(): Rail[T] {
        return toChunk(range).toRail();
    }

    public def last() {
	return range.max;
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

    public def getFirst(): T  = base(range.min);
    public def getLast(): T  = base(range.max);

    public def removeAt(i0: Long): T {
	throw new UnsupportedOperationException();
    }

    public def removeFirst(): T {
	throw new UnsupportedOperationException();
    }
    public def removeLast(): T {
	throw new UnsupportedOperationException();
    }

    public operator this(i: Long): T = base(i); 
    public operator this(i: Long)=(v: T) : T = set(v,i);
    
    public def set(v: T, i0: Long): T {
        base(i0) = v;
        return v;
    }

    public def get(i0: Long): T {
	return base(i0);
    }

    public def size(): Long = range.max-range.min+1;
    public def isEmpty(): Boolean = range.max<range.min; 
    public def indices(): List[Long] {
	val result = new ArrayList[Long]();
	for(i in range) result.add(i);
	return result;
    }
    public def reverse() {
	throw new UnsupportedOperationException();
    }
    public def sort() { throw new UnsupportedOperationException();} 
    public def sort(cmp:(T,T)=>Int) { throw new UnsupportedOperationException();} 

    private static class It[S] (base:List[S], range:LongRange)implements ListIterator[S] {
        
        private var i: Long = range.min-1; // offset inside the chunk
        
        public def hasNext(): Boolean {
            return i+1 <= range.max;
        }
        public def next(): S {
            return base(++i);
        }
        public def nextIndex(): Long {
            return ++i;
        }

        public def hasPrevious(): Boolean {
            return i -1 >= range.min;
        }

        public def previousIndex(): Long {
            return (--i);
        }
        
        public def previous(): S {
            return base(--i);
        }
        
        public def remove(): void {
	    throw new UnsupportedOperationException();
        }
        
        public def set(v: S): void {
            base(i) = v;
        }
        
        public def add(v: S): void {
	    throw new UnsupportedOperationException();
        }
    }
    public def iterator(): ListIterator[T] {
        return new It[T](this.base,this.range);
    }
    public def iteratorFrom(index:Long): ListIterator[T] {
        return new It[T](this.base, Math.max(range.min,index)..range.max);
    }
    public def subList(fromIndex:Long, toIndex:Long): RangedList[T] {
	return new RangedListView[T](base, Math.max(fromIndex,range.min)..Math.min(toIndex,range.max));
    }

    public def each(op: (T)=>void) {
	if(base instanceof RangedList[T]) {
	    (base as RangedList[T]).each(this.range, op);
	} else {
	    for(i in this.range) {
		op(base(i));
	    }
	}
    }

    public def each(range:LongRange, op: (T)=>void) {
	if(base instanceof RangedList[T]) {
	    (base as RangedList[T]).each(Math.max(range.min,this.range.min)..Math.min(range.max,this.range.max), op);
	} else {
	    for(i in Math.max(range.min,this.range.min)..Math.min(range.max,this.range.max)) {
		op(base(i));
	    }
	}
    }
    
    public def each[U](op: (T,Receiver[U])=>void, receiver: Receiver[U]) {
	val op2 = (t:T)=>{ op(t,receiver); };
	each(this.range, op2);
    }

    public def each[U](range:LongRange, op: (T,Receiver[U])=>void, receiver: Receiver[U]) {
	val op2 = (t:T)=>{ op(t,receiver); };
	each(this.range, op2);
    }

    private def this(ds: Deserializer) {
        val chunk = ds.readAny() as Chunk[T];
        base = chunk;
        range = chunk.getRange();
    }

    public def serialize(s:Serializer) {
//        Console.OUT.println("[" + here.id + "] serialize RangedListView " + range);
        val chunk = toChunk(range);
	s.writeAny(chunk);
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
	var c :Long = 0;
        for (i in range) {
            if (c++ > 0) sb.add(",");
            sb.add("" + this(i));
	    if (c == sz) break;
        }
        if (sz < size()) sb.add("...(omitted " + (size() - sz) + " elements)");
        sb.add("@" + range.min + ".."+last()+"]");
        return sb.toString();
    }

    public static def main(args:Rail[String]) {
        val i = 10;
        val c = new Chunk[Long]((10*i)..(11*i-1));
        Console.OUT.println("prepare:"+c);
        for(j in 0..(i-1)) {
            Console.OUT.println("set@"+(10*i+j));
            c(10*i+j) = 100*i+10*j;
        }
        Console.OUT.println("Chunk :"+ c);
        val r1 = c.subList(10*i+0, 10*i+2);
        val r2 = c.subList(10*i+2, 10*i+8);
        val r3 = c.subList(10*i+8, 10*i+9);
        val r0 = c.subList(10*i+0, 10*i+9);
        Console.OUT.println("RangedListView :" + r1);
        Console.OUT.println("RangedListView :" + r2);
        Console.OUT.println("RangedListView :" + r3);
        Console.OUT.println("RangedListView :" + r0);
    }
}



