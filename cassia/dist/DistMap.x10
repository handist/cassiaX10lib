package cassia.dist;

import x10.util.Set;
import x10.util.Map;
import x10.util.HashMap;
import x10.util.Team;
import x10.util.ArrayList;
import x10.util.List;
import x10.io.Deserializer;
import x10.io.Serializer;
import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;
import x10.util.StringBuilder;



/**
 * A Map data structure spread over the multiple places.
 */
public class DistMap[T,U] {U haszero} extends AbstractDistCollection[Map[T, U]] implements Relocatable {

    /**
     * Construct a DistMap.
     */
    public def this() {
        this(Place.places(), Team.WORLD);
    }

    /**
     * Construct a DistMap with the given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
        //this(placeGroup, () => new HashMap[T,U](1 << 5));
        this(placeGroup, team, () => new HashMap[T,U]());
    }

    /**
     * Construct a DistMap with the given arguments.
     *
     * @param placeGroup PlaceGroup.
     * @param init the function used in initialization.
     */
    public def this(placeGroup: PlaceGroup, team: Team, init: ()=>Map[T,U]) {
        this(placeGroup, (): Local[Map[T,U]] => {
            return new Local[Map[T,U]](placeGroup, team, init());
        });
    }

    def this(placeGroup: PlaceGroup, init: ()=>Local[Map[T,U]]) {
        super(placeGroup, init);
    }

    /**
     * Remove the all local entries.
     */
    public def clear(): void {
        this.data.clear();
    }

    /**
     * Return the PlaceGroup.
     *
     * @return the PlaceGroup.
     */
    // public def placeGroup(): PlaceGroup = placeGroup;

    /**
     * Return the number of the local entries.
     *
     * @return the number of the local entries.
     */
    public def size(): Long {
        return data.size();
    }

    /**
     * Return the value corresponding to the specified key.
     * If the specified entry is not found,
     * return the value of Zero.get[U]().
     *
     * @param key the key corresponding to the value.
     * @return the value corresponding to the specified key.
     */
    public operator this(key: T): U {
        return data(key);
    }

    /**
     * Return the value corresponding to the specified key.
     * If the specified entry is not found,
     * return the value of Zero.get[U]().
     *
     * @param key the key corresponding to the value.
     * @return the value corresponding to the specified key.
     */
    public def get(key: T): U {
        return data.get(key);
    }

    /**
     * Put a new entry.
     *
     * @param key the key of the new entry.
     * @param value the value of the new entry.
     */
    public operator this(key: T) = (value: U): U {
        return put(key, value);
    }

    /**
     * Put a new entry.
     *
     * @param key the key of the new entry.
     * @param value the value of the new entry.
     */
    public def put(key: T, value: U): U {
        return data(key) = value;
    }

    private def putForMove(key: T, value: U): U {
        if (data.containsKey(key)) {
	    throw new Exception("DistMap cannot override existing entry: " + key);
	}
        return data(key) = value;
    }

    public def delete(key: T): Boolean {
        return data.delete(key);
    }

    /**
     * Remove the entry corresponding to the specified key.
     *
     * @param key the key corresponding to the value.
     */
    public def remove(key: T): U {
        return data.remove(key);
    }

    /**
     * Apply the same operation onto the all local entries.
     *
     * @param op the operation.
     */
    public def each(op: (T,U)=>void): void {
        for (entry in entries()) {
            op(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Apply the same operation onto the all elements including other place and create a new DistMap which consists of the results of the operation.
     *
     * @param op the operation.
     * @return a DistMap which consists of the results of the operation.
     */
    public def map[S](op: (U)=>S) {S haszero}: DistMap[T,S] {
        return new DistMap[T,S](placeGroup, team, () => {
            val dst = new HashMap[T,S]();
            for (entry in entries()) {
                val key = entry.getKey();
                val value = entry.getValue();
                dst(key) = op(value);
            }
            return dst;
        });
    }

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param op the operation.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public def reduce(op: (U,U)=>U, unit:U): U = reduce[U](op, op, unit);

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param lop the operation using in the local reduction.
     * @param gop the operation using in the reduction of the results of the local reduction.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public def reduce[S](lop: (S,U)=>S, gop: (S,S)=>S, unit: S): S {
        val reducer = new Reducible[S]() {
            public def zero() = unit;
            public operator this(a: S, b: S) = gop(a, b);
        };
        return finish (reducer) {
            placeGroup.broadcastFlat(() => {
                offer(reduceLocal(lop, unit));
            });
        };
    }

    /**
     * Reduce the all local elements using the given operation.
     *
     * @param op the operation using in the reduction.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public def reduceLocal[S](op: (S,U)=>S, unit: S): S {
        var accum: S = unit;
        for (entry in data.entries()) {
            accum = op(accum, entry.getValue());
        }
        return accum;
    }

    /*
     * Return true if the specified entry is exist at local.
     *
     * @param key a key.
     * @return true or false.
     */
    public def containsKey(key: T): Boolean {
        return data.containsKey(key);
    }

    /**
     * Return the Set of local keys.
     *
     * @return the Set of local keys.
     */
    public def keySet(): Set[T] {
        return data.keySet();
    }

    /**
     * Return the Set of local entries.
     *
     * @return the Set of local entries.
     */
    public def entries(): Set[Map.Entry[T,U]] {
        return data.entries();
    }

    /**
     * Request that the specified element is relocated when #sync is called.
     *
     * @param key the key of the relocated entry.
     * @param pl the destination place.
     * @param mm MoveManagerLocal
     */
    public def moveAtSync(key: T, pl: Place, mm:MoveManagerLocal) {U haszero}: void {
        if (pl == here) return;
	val toBranch = this; // using plh@AbstractCol
        val serialize = (s: Serializer) => {
            val value = this.remove(key);
            s.writeAny(key);
            s.writeAny(value);
        };
        val deserialize = (ds: Deserializer) => {
            val key = ds.readAny() as T;
            val value = ds.readAny() as U;
            toBranch.putForMove(key, value);
        };
        mm.request(pl, serialize, deserialize);
    }

    public def moveAtSync(keys: List[T], pl: Place, mm:MoveManagerLocal) {U haszero}: void {
        if (pl == here) return;
	val collection = this;
	val serialize = (s: Serializer) => {
	    val size = keys.size();
	    s.writeAny(size);
	    for (key in keys) {
	        val value = collection.remove(key);
		s.writeAny(key);
		s.writeAny(value);
	    }
	};
	val deserialize = (ds: Deserializer) => {
	    val size = ds.readAny() as Long;
	    for (i in 1..size) {
	        val key = ds.readAny() as T;
		val value = ds.readAny() as U;
		collection.putForMove(key, value);
	    }
	};
	mm.request(pl, serialize, deserialize);	
    }

    public def moveAtSync(rule:(T)=>Place, mm:MoveManagerLocal) {U haszero}: void {
        val collection = this;
	val keysToMove = new HashMap[Place, ArrayList[T]]();
	collection.each((key: T, value: U) => {
	    val destination = rule(key);
	    if (!keysToMove.containsKey(destination)) {
	        keysToMove(destination) = new ArrayList[T]();
	    }
	    keysToMove(destination).add(key);
	});
	val places = keysToMove.keySet();
	for (place in places) {
	    moveAtSync(keysToMove(place), place, mm);
	}
    }

    public def moveAtSync(dist:Distribution[T], mm:MoveManagerLocal) {U haszero}: void {
        moveAtSync((key: T) => dist.place(key), mm);
    }

    public def relocate(rule: (T)=>Place, mm:MoveManagerLocal): void {
        for (key in keySet()) {
            val place = rule(key);
            moveAtSync(key, place, mm);
        }
        mm.sync();
    }

    public def relocate(rule: (T)=>Place): void {
        relocate(rule, new MoveManagerLocal(placeGroup, team));
    }

    public def balance(): void {
        finish for(p in placeGroup){
            at(p) async{
                val al = new ArrayList[Map.Entry[T,U]](entries().size()).make(entries());
                val balance = new LoadBalancer[Map.Entry[T,U]](al, placeGroup, team);
                balance.execute();
                Console.OUT.println(here + " balance.check1");
                clear();
                Console.OUT.println(here + " balance.check2");
                Console.OUT.println(here + " balance.ArrayList.size() : " + al.size());
                var total:long = 0;
                var count:long = 0;
                var time:long;
                time = - System.nanoTime();
                for(i in al){
                    val key = i.getKey();
                    val value = i.getValue();
                    var time2:long;
                    time2 = - System.nanoTime();
                    put(key, value);
                    time2 += System.nanoTime();
                    total += time2;
                    count++;
                    // if(here == Place.places()(Place.places().numPlaces() - 1))
                    //  Console.OUT.println(here + " key : " + i.getKey() + ", value : " + i.getValue());
                }
                time += System.nanoTime();
                Console.OUT.println(here + " count : " + (count) + " ms");
                Console.OUT.println(here + " put : " + (total/(1000000)) + " ms");
                Console.OUT.println(here + " for : " + (time/(1000000)) + " ms");
                Console.OUT.println(here + " data.size() : " + size());
                Console.OUT.println(here + " balance.check3");
            }
        }
    }

    public def integrate(src : Map[T, U]) {
        for(e in src.entries()){
            put(e.getKey(), e.getValue());
        }
    }

    public def printLocalData(){
        val sb = new StringBuilder();
        sb.add("at "+ here + "\n");
        for(e in data.entries()){
            sb.add("key : "+e.getKey() + ", value : " + e.getValue() + "\n");
        }
        Console.OUT.println(sb.result());
    }

}
