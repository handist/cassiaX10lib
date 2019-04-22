package samples.rangedList;
import x10.util.*;
import cassia.util.*;

public class SimpleReceiver[T] implements Receiver[T] {

    private val holder: ArrayList[T];

    public def this() {
        holder = new ArrayList[T]();
    }

    public def receive(v: T) {
        holder.add(v);
    }

    public def close() {
    }

    public def size() {
        return holder.size();
    }

    public def getContainer() {
        return holder;
    }

    public def equals(that: SimpleReceiver[T]) {
        val tmp = that.getContainer().clone();
	var c: Long = 0;
        for (v in holder) {
	    val idx = tmp.indexOf(v);
	    if (idx == -1) {
	        return false;
	    }
 	    tmp.removeAt(idx);
	    c++;
	}
//	Console.OUT.println("SimpleReceiver#equals " + c + " items checked");
	return tmp.isEmpty();
    }

    public def toString() {
        val sb = new x10.util.StringBuilder();
        sb.add("[");
	val sz = Config.omitElementsToString ? Math.min(holder.size(), Config.maxNumElementsToString) : holder.size();
        for (var i: Long = 0; i < sz; i++) {
	    if (i > 0) sb.add(",");
	    sb.add("" + holder(i));
        }
	if (sz < holder.size()) sb.add("...(omitted " + (holder.size() - sz) + " elements)");
	sb.add("]");
	return sb.toString();
    }
}
