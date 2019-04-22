package cassia.dist;
import cassia.concurrent.Pool;
import x10.util.concurrent.AtomicLong;
import x10.util.concurrent.Condition;
import x10.util.concurrent.SPMDBarrier;
import x10.util.Indexed;
import x10.util.List;
import cassia.util.*;


class ParallelAccumulator {

    private static class RangeRestrictedIterator[T] implements Iterator[T] {

        // using iterator of LongRange is one way
        // however, it affects performance
        var current: Long;
        val max: Long;
        val indexed: Indexed[T];

        def this(range: LongRange, indexed: Indexed[T]) {
            this.current = range.min;
            this.max = range.max;
            this.indexed = indexed;
        }

        public def hasNext(): Boolean {
            return current <= max;
        }

        public def next(): T {
            return indexed(current++);
        }
    }

    private static class RangeRestrictedIterable[T] implements Iterable[T] {

        val range: LongRange;
        val indexed: Indexed[T];

        def this(range: LongRange, indexed: Indexed[T]) {
            this.range = range;
            this.indexed = indexed;
        }

        public def iterator(): Iterator[T] {
            return new RangeRestrictedIterator[T](range, indexed);
        }
    }

    private def this() {}

    private static def parallelize(pool: Pool, nth: Long, closure: ()=>void): void {
        if (pool == null) {
            finish for (var i: Long = 0; i < nth; i++) {
                async {
                    closure();
                }
            }
            return;
        }
        val condition = new Condition();
        val count = new AtomicLong(1);
        for (var i: Long = 1; i < nth; i++) {
            pool.submit(() => {
                closure();
                if (count.incrementAndGet() == nth) {
                    condition.release();
                }
            });
        }
        closure();
        condition.await();
    }

    private static def parallelizeAsync(pool: Pool, nth: Long, closure: ()=>void): Condition {
        val condition = new Condition();
        val count = new AtomicLong(0);
        if (pool == null) {
            for (var i: Long = 0; i < nth; i++) {
                async {
                    closure();
                    if (count.incrementAndGet() == nth) {
                        condition.release();
                    }
                }
            }
            return condition;
        }
        for (var i: Long = 0; i < nth; i++) {
            pool.submit(() => {
                closure();
                if (count.incrementAndGet() == nth) {
                    condition.release();
                }
            });
        }
        return condition;
    }

    private static def checkNthreads(nth: Long): void {
        if (nth <= 0) {
            throw new IllegalArgumentException("the number of threads must be larger than 0 (given = " + nth + ")");
        }
    }

    static def execute[T](pool: Pool, data: Indexed[T], nth: Long, op: (Iterable[T])=>void){T haszero}: void {
        checkNthreads(nth);
        val ntasks = nth;
        val base = data.size() / ntasks;
        val rem = data.size() % ntasks;
        val queue = new AtomicLong(0);
        parallelize(pool, nth, (): void => {
            val taskId = queue.getAndIncrement();
            // if taskId < rem; then from = base * taskId + taskId; else from = base * taskId + rem;
            val from = base * taskId + Math.min(taskId, rem);
            // if taskId < rem; then count = base + 1; else count = base;
            val count = base + Math.max(0, Math.min(rem - taskId, 1));
            val range = (from..(from + count - 1));
            val iterable = new RangeRestrictedIterable[T](range, data);
            op(iterable);
        });
    }

    static def executeAsync[T](pool: Pool, data: Indexed[T], nth: Long, op: (Iterable[T])=>void){T haszero}: Condition {
        checkNthreads(nth);
        val ntasks = nth;
        val base = data.size() / ntasks;
        val rem = data.size() % ntasks;
        val queue = new AtomicLong(0);
        return parallelizeAsync(pool, nth, (): void => {
            val taskId = queue.getAndIncrement();
            val from = base * taskId + Math.min(taskId, rem);
            val count = base + Math.max(0, Math.min(rem - taskId, 1));
            val range = (from..(from + count - 1));
            val iterable = new RangeRestrictedIterable[T](range, data);
            op(iterable);
        });
    }

    static def execute[T, U](pool: Pool, data: ChunkedList[T], receiverHolder: ReceiverHolder[U], nth: Long, op: (T, Receiver[U])=>void){T haszero}: void {
        checkNthreads(nth);
	val alltasks = data.separate(nth);
        //val base = data.size() / ntasks;
        //val rem = data.size() % ntasks;
        val queue = new AtomicLong(0);
        parallelize(pool, nth, (): void => {
            val receiver = receiverHolder.getReceiver();
	    val op2 = (t:T)=>{ op(t,receiver); };
            while (true) {
                val taskId = queue.getAndIncrement();
                if (taskId >= nth) {
                    break;
                }
                // if taskId < rem; then from = base * taskId + taskId; else from = base * taskId + rem;
		val mytasks = alltasks(taskId);
		mytasks.each(op2);
            }
            receiver.close();
        });
    }

    static def executeAsync[T, U](pool: Pool, data: ChunkedList[T], receiverHolder: ReceiverHolder[U], nth: Long, op: (T, Receiver[U])=>void){T haszero}: Condition {
        checkNthreads(nth);
	val alltasks = data.separate(nth);
        //val ntasks = data.size();
        //val base = data.size() / ntasks;
        //val rem = data.size() % ntasks;
        val queue = new AtomicLong(0);
        return parallelizeAsync(pool, nth, (): void => {
            val receiver = receiverHolder.getReceiver();
	    val op2 = (t:T)=>{ op(t,receiver); };
            while (true) {
                val taskId = queue.getAndIncrement();
                if (taskId >= nth) {
                    break;
                }
		val mytasks = alltasks(taskId);
		mytasks.each(op2);
            }
            receiver.close();
        });
    }
}
