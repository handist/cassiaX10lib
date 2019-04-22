package samples.dist;

import x10.util.*;

public class  WithHistory[T] {
    private var data: T;
    private val history: ArrayList[T];

    public def this(v: T) {
        history = new ArrayList[T]();
        data = v;
    }

    public def set(v: T): T {
        history.add(data);
        data = v;
        return v;
    }

    public operator this()=(v: T): T = set(v);

    public operator this(): T = data;

    public def getHistory(): ArrayList[T] = history;

    public def toString(): String =  data.toString();

    public def equals(that: WithHistory[T]): boolean {
        if (!data.equals(that())) {
            return false;
        }
        val thatHistory = that.getHistory();
        if (history.size() != thatHistory.size()) {
            return false;
        }
        val thisHistoryIt = history.iterator();
        val thatHistoryIt = thatHistory.iterator();
        while (thisHistoryIt.hasNext()) {
            if (!thisHistoryIt.next().equals(thatHistoryIt.next())) {
                return false;
            }
        }
        return true;
    }

    public def equals(that: Any): boolean {
        if (that instanceof WithHistory[T]) {
            val thatWithHistory = that as WithHistory[T];
            return equals(thatWithHistory);
        } else {
            return false;
        }
    }
}
