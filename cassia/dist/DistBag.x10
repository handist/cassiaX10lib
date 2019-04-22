package cassia.dist;

import cassia.util.*;
import cassia.concurrent.Pool;
import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;
import x10.util.concurrent.Lock;
import x10.util.Container;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Team;
import x10.xrx.Runtime;
import x10.io.Serializer;
import x10.io.Deserializer;



/**
 * A class for handling objects at multiple places.
 * It is allowed to add new elements dynamically.
 * This class provides the method for load balancing.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o There is only one load balancing method.
 *   The method flattens the number of elements of the all places.
 */
public class DistBag[T] extends AbstractDistCollection[List[T]] implements Container[T], ReceiverHolder[T] {

    @TransientInitExpr(getReceiversInternal())
    transient val receivers: Rail[DistBagReceiver[T]];

    private final def getReceiversInternal(): Rail[DistBagReceiver[T]] {
        val local = getLocal[DistBagLocal[T]]();
        if (local == null) {
            return null;
        }
        return local.receivers;
    }

    /**
     * Create a new DistBag.
     * Place.places() is used as the PlaceGroup.
     */
    public def this() {
        this(Place.places(), Team.WORLD);
    }

    /**
     * Create a new DistBag using the given arguments.
     *
     * @param placeGroup a PlaceGroup.
     * @param team a Team.
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
        this(placeGroup, team, () => new ArrayList[T]());
    }

    def this(placeGroup: PlaceGroup, team: Team, init: ()=>List[T]) {
        super(placeGroup, (): Local[List[T]] => {
            val list = init();
            val local = new DistBagLocal[T](placeGroup, team, list);
            return local;
        });
        this.receivers = getReceiversInternal();
    }

    /**
     * Add new element.
     *
     * @param v a new element.
     */
    public def add(v: T): Boolean = data.add(v);

    /**
     * Add all elements int the given list.
     *
     * @param list the list.
     */
    public def addAll(list: List[T]): Boolean {
        return this.data.addAll(list);
    }

    /**
     * Remove a element at the local storage.
     */
    public def remove(): T = data.removeLast();

    /**
     * Clear the local elements.
     */
    public def clear(): void {
        data.clear();
    }

    /**
     * Return whether DistBag's local storage has no value.
     *
     * @return true if DistBag's local storage has no value.
     */
    public def isEmpty(): Boolean {
        return data.isEmpty();
    }

    /**
     * Return whether the DistBag contains the given value.
     * This method uses the T#equals to evaluate the equality.
     *
     * @param v a value of type T.
     * @return true or false.
     */
    public def contains(v: T): Boolean = data.contains(v);

    /**
     * Return whether the DistBag contains all the values of the given Container.
     * This method used the T#equals to evaluate the equality.
     *
     * @param container a Container[T].
     * @return true if DistBag contains the all given values.
     */
    public def containsAll(container: Container[T]): Boolean {
        return data.containsAll(container);
    }

    /**
     * Return the number of local elements.
     *
     * @return the number of local elements.
     */
    public def size(): Long {
        return data.size();
    }

    /**
     * Return a Container that has the same values of DistBag's local storage.
     *
     * @return a Container that has the same values of local storage.
     */
    public def clone(): Container[T] {
        return data.clone();
    }

    /**
     * Return the iterator for the local elements.
     *
     * @return the iterator.
     */
    public def iterator(): Iterator[T] = this.data.iterator();

    public def getReceiver(): Receiver[T] {
        val id = Runtime.workerId();
        if (receivers(id) == null) {
            receivers(id) = new DistBagReceiver[T](this);
        }
        return receivers(id);
    }

    /**
     * gather all place-local elements to the root Place.
     *
     * @param root the place where the result of reduction is stored.
     */
    public def gather(root: Place): void {
	val serProcess = (ser:Serializer) => {
	    ser.writeAny(this.data);
	};
	val desProcess = (des:Deserializer, place:Place) => {
	    val imported = des.readAny() as List[T];
	    this.data.addAll(imported);
	};
	CollectiveRelocator.gatherSer(placeGroup, team, placeGroup(0), serProcess, desProcess);
        if (here != root) {
            clear();
        }
    }

    public def balance(): void {
        throw new UnsupportedOperationException();
    }

    public def integrate(src : List[T]) {
        throw new UnsupportedOperationException();
    }

    static class DistBagLocal[T] extends Local[List[T]] {

        def this(placeGroup: PlaceGroup, team: Team, data: List[T]) {
            super(placeGroup, team, data);
        }

        val receivers: Rail[DistBagReceiver[T]] = new Rail[DistBagReceiver[T]](Runtime.MAX_THREADS as Long);
    }


    static class DistBagReceiver[T] implements Receiver[T] {

        val distBag: DistBag[T];
        val buffer: List[T] = new ArrayList[T]();

        def this(distBag: DistBag[T]) {
            this.distBag = distBag;
        }

        public def receive(value: T): void {
            buffer.add(value);
        }

        public def close(): void {
            distBag.lock();
            distBag.data.addAll(buffer);
            buffer.clear();
            distBag.unlock();
        }
    }
}
