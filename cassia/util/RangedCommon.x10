package cassia.util;
import x10.util.*;

public interface RangedCommon[T] {
    public def getRange():LongRange;
    public def cloneRange(newRange: LongRange): RangedList[T];
    public def toChunk(newRange: LongRange): Chunk[T];
    public def toRail(): Rail[T];
    public def splitRange(splitPoint: Long): Pair[RangedList[T], RangedList[T]];
    public def splitRange(splitPoint1: Long, splitPoint2: Long): Triplets[RangedList[T], RangedList[T], RangedList[T]];

    public def each(op: (T)=>void):void;
    public def each(range:LongRange, op: (T)=>void):void;
    //   public def each[U](op: (T,Receiver[U])=>void, receiverHolder: ReceiverHolder[U]):void;
    //public def each[U](range:LongRange, op: (T,Receiver[U])=>void, receiverHolder: ReceiverHolder[U]):void;

}
