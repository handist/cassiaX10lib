package cassia.concurrent;
import x10.compiler.Uncounted;
import x10.util.concurrent.AtomicBoolean;
import x10.util.concurrent.Lock;
import x10.util.GrowableRail;
import x10.util.HashSet;
import x10.util.Set;
import x10.xrx.Runtime;
import x10.xrx.Worker;

public class Pool {

    private static val pools: GrowableRail[Pool] = new GrowableRail[Pool]();

    private val terminate: AtomicBoolean = new AtomicBoolean(false);
    private val queue: Queue[()=>void] = new Queue[()=>void]();
    private val lock: Lock = new Lock();
    private val waitings: Set[Long] = new HashSet[Long]();
    private var localQueues: Rail[Queue[()=>void]] = null;
    private var localLocks: Rail[Lock] = null;
    private var workers: Rail[Worker] = null;
    private var nthreads: Long;

    public static def getInstance(): Pool {
        if (pools.size() == 0) {
            pools.add(new Pool());
        }
        return pools(0);
    }

    private def this() {}

    /**
     * hold the specified number of workers.
     *
     * @param nthreads the number of workers.
     */
    public def hold(nthreads: Int): void {
        hold(nthreads as Long);
    }

    /**
     * hold the specified number of workers.
     *
     * @param nthreads the number of workers.
     */
    public def hold(nthreads: Long): void {
        hold(nthreads, (id: Long) => id);
    }

    private def hold(nthreads: Long, assign: (Long)=>Long): void {
        this.nthreads = nthreads;
        this.localQueues = new Rail[Queue[()=>void]](nthreads, (Long) => new Queue[()=>void]());
        this.localLocks = new Rail[Lock](nthreads, (Long) => new Lock());
        this.workers = new Rail[Worker](nthreads, (Long) => null);
        this.terminate.compareAndSet(true, false);
        for (id in 0..(nthreads - 1)) {
            @Uncounted async {
                // Affinity.bind(assign(id));
                while (loop(id));
            }
        }
        while (waitings.size() < nthreads) {
            System.sleep(0);
        }
    }

    // polling function
    private def loop(id: Long): Boolean {
        var task: ()=>void = null;
        val localQueue = localQueues(id);
        val localLock = localLocks(id);
        // try local queue at first
        localLock.lock();
        task = localQueue.poll();
        if (task != null) {
            localLock.unlock();
            task();
            Unsafe.dealloc(task);
            return true;
        }
        // try shared queue
        lock.lock();
        task = queue.poll();
        if (task != null) {
            lock.unlock();
            localLock.unlock();
            task();
            Unsafe.dealloc(task);
            return true;
        }
        if (terminate.get()) {
            // exit polling
            lock.unlock();
            localLock.unlock();
            return false;
        }
        // park worker when no task is found
        val worker = Runtime.worker();
        workers(id) = worker;
        waitings.add(id);
        lock.unlock();
        localLock.unlock();
        Worker.park();
        return true;
    }

    /**
     * submit a task and assign it to the specified worker.
     *
     * @param assignedId the worker's id.
     * @param task a submitted task.
     */
    public def submit(assignedId: Long, task: ()=>void): void {
        val localQueue = localQueues(assignedId);
        val localLock = localLocks(assignedId);
        // push task to local queue.
        localLock.lock();
        if (terminate.get()) {
            localLock.unlock();
            return;
        }
        localQueue.push(() => {
            task();
        });
        lock.lock();
        // unpark specified worker when it is parked
        val worker = workers(assignedId);
        if (worker != null) {
            workers(assignedId) = null;
            waitings.remove(assignedId);
            lock.unlock();
            localLock.unlock();
            worker.unpark();
            return;
        }
        lock.unlock();
        localLock.unlock();
    }

    /*
     * submit a task to the shared queue.
     *
     * @param task a submitted task.
     */
    public def submit(task: ()=>void): void {
        // check termination
        lock.lock();
        if (terminate.get()) {
            lock.unlock();
            return;
        }
        queue.push(() => {
            task();
        });
        if (waitings.size() > 0) {
            // unpark a worker
            val id = waitings.iterator().next();
            waitings.remove(id);
            val worker = workers(id);
            workers(id) = null;
            lock.unlock();
            worker.unpark();
            return;
        }
        lock.unlock();
    }

    /**
     * release all workers.
     */
    public def release(): void {
        lock.lock();
        // wait for all workers being parked
        while (waitings.size() < nthreads) {
            val worker = Runtime.worker();
            submit(() => { worker.unpark(); });
            lock.unlock();
            Worker.park();
            lock.lock();
        }
        terminate.set(true);
        // unpark all workes
        while (waitings.size() > 0) {
            val id = waitings.iterator().next();
            waitings.remove(id);
            val worker = workers(id);
            workers(id) = null;
            lock.unlock();
            worker.unpark();
            lock.lock();
        }
        lock.unlock();
    }

    public def count(): Long {
        return nthreads;
    }
}
