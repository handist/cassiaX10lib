package cassia.concurrent;
import x10.compiler.Uncounted;
import x10.util.concurrent.Lock;
import x10.xrx.Runtime;
import x10.xrx.Worker;

/**
 * dedicated worker.
 */
public class DedicatedWorker {

    private val queue: Queue[()=>void] = new Queue[()=>void]();
    private val lock: Lock = new Lock();
    private var terminate: Boolean = false;
    private var worker: Worker = null;

    /**
     * Start worker.
     */
    public def start(): void {
        @Uncounted async {
            while (loop());
        }
    }

    private def loop(): Boolean {
        lock.lock();
        if (terminate) {
            lock.unlock();
            return false; // break while loop
        }
        val task = queue.poll();
        if (task != null) {
            lock.unlock();
            task();
            return true; // continue while loop
        }
        worker = Runtime.worker();
        lock.unlock();
        Worker.park();
        return true; // continue while loop
    }

    /**
     * Stop worker.
     */
    public def stop(): void {
        lock.lock();
        terminate = true;
        if (worker != null) {
            val tmp = worker;
            worker = null;
            lock.unlock();
            tmp.unpark();
            return;
        }
        lock.unlock();
    }

    /**
     * Submit a given task.
     *
     * @param task a submitted closure.
     */
    public def submit(task: ()=>void): void {
        lock.lock();
        if (terminate) {
            lock.unlock();
            return;
        }
        queue.push(task);
        if (worker != null) {
            val tmp = worker;
            worker = null;
            lock.unlock();
            tmp.unpark();
            return;
        }
        lock.unlock();
    }
}
