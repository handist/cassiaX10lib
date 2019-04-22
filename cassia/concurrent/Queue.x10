package cassia.concurrent;
import x10.util.concurrent.Lock;

/**
 * A class that implements FIFO data structure.
 */
public class Queue[T] {T isref, T haszero} {

    private val pollLock: Lock = new Lock();
    private val pushLock: Lock = new Lock();
    private var head: Node[T] = null;
    private var tail: Node[T] = null;

    static private class Node[S] {S isref, S haszero} {

        public var value: S;
        public var next: Node[S];

        public def this(value: S, next: Node[S]) {
            this.value = value;
            this.next = next;
        }
    }

    public def push(v: T): Boolean {
        val newNode = new Node[T](v, null);
        pushLock.lock();
        if (tail == null) {
            tail = newNode;
            head = newNode;
            pushLock.unlock();
            return true;
        }
        if (head == null) {
            tail.next = newNode;
            tail = newNode;
            head = newNode;
            pushLock.unlock();
            return true;
        }
        tail.next = newNode;
        tail = newNode;
        pushLock.unlock();
        return true;
    }

    public def poll(): T {
        pollLock.lock();
        if (head == null) {
            pollLock.unlock();
            return null;
        }
        val tmp = head;
        if (tmp == tail) {
            pushLock.lock();
            if (tmp == tail) {
                head = null;
                tail = null;
                pushLock.unlock();
                pollLock.unlock();
                val value = tmp.value;
                Unsafe.dealloc(tmp);
                return value;
            }
            pushLock.unlock();
        }
        head = tmp.next;
        pollLock.unlock();
        val value = tmp.value;
        Unsafe.dealloc(tmp);
        return value;
    }

    public def empty(): Boolean {
        return (head == null);
    }
}
