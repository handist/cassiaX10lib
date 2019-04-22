package cassia.util;
import x10.util.*;

public interface RangedList[T] extends List[T], RangedCommon[T]/*,RangedIndexed[T],RangedSettable[T]*/ {
    public def subList(from:Long,to:Long): RangedList[T];
}
