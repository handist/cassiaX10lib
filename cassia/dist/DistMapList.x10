package cassia.dist;

import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;
import x10.io.Deserializer;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Set;
import x10.util.Map;
import x10.util.HashMap;
import x10.util.Team;
import x10.io.Deserializer;
import x10.io.Serializer;



/**
 * A Map data structure spread over the multiple places.
 * This class allows multiple values for one key.
 */
public class DistMapList[T,U] extends DistMap[T, List[U]] implements Relocatable {

    /**
     * Construct a DistMapList.
     */
    public def this() {
        this(Place.places(), Team.WORLD);
    }

    /**
     * Construct a DistMapList with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
        this(placeGroup, team, () => new HashMap[T,List[U]]());
    }

    /**
     * Construct a DistMapList with given arguments.
     *
     * @param placeGroup PlaceGroup.
     * @param init the function used in initialization.
     */
    public def this(placeGroup: PlaceGroup, team: Team, init: ()=>Map[T,List[U]]) {
        super(placeGroup, team, init);
    }

    /**
     * Put a new value to the list of specified entry.
     *
     * @param key the key of the entry.
     * @param value the new value.
     */
    public def put(key: T, value: U): Boolean {
        var list: List[U] = data(key);
        if (list == Zero.get[List[U]]()) {
            list = new ArrayList[U]();
            data(key) = list;
        }
        return list.add(value);
    }

    public def putForMove(key: T, values: Rail[U]): Boolean {
        var list: List[U] = data(key);
        if (list == Zero.get[List[U]]()) {
            list = new ArrayList[U]();
            data(key) = list;
        }
	val alist = list as ArrayList[U];
        return alist.addAll(values);
    }

    /**
     * Remove the entry corresponding to the specified key.
     *
     * @param key the key corresponding to the value.
     */
    public def removeForMove(key: T): Rail[U] {
        val list = data(key) as ArrayList[U];
        val rail = list.toRail();
	data(key).clear();
        return rail;
    }

    /**
     * Request that the specified value is put to the list corresponding to the given key when #sync is called.
     *
     * @param key the key of the list.
     * @param pl the destination place.
     * @param mm MoveManagerLocal
     */
    public def putAtSync(key: T, value: U, pl: Place, mm:MoveManagerLocal) {
	val toBranch = this; // using plh@AbstractCol
        val serialize = (s: Serializer) => {
            s.writeAny(key);
            s.writeAny(value);
        };
        val deserialize = (ds: Deserializer) => {
            val key = ds.readAny() as T;
            val value = ds.readAny() as U;
            toBranch.put(key, value);
        };
        mm.request(pl, serialize, deserialize);
    }

    public def moveAtSync(key: T, pl: Place, mm:MoveManagerLocal) {
        if(pl == here) return;
	if(!containsKey(key)) throw new Exception("DistMapList cannot move uncontained entry: "+key);
	val toBranch = this; // using plh@AbstractCol
        val serialize = (s: Serializer) => {
	    val value = this.removeForMove(key);
            s.writeAny(key);
            s.writeAny(value);
        };
        val deserialize = (ds: Deserializer) => {
            val key = ds.readAny() as T;
            val value = ds.readAny() as Rail[U];
            toBranch.putForMove(key, value);
        };
        mm.request(pl, serialize, deserialize);
    }

    /**
     * Apply the same operation onto the all local entries.
     *
     * @param op the operation.
     */
    public def each(op: (T,U)=>void) {
        for (entry in data.entries()) {
            val key = entry.getKey();
            for (value in entry.getValue()) {
                op(key, value);
            }
        }
    }

    /**
     * Apply the same operation onto the all elements including other places and create new DistMapList which consists of the results of the operation.
     *
     * @param op the operation.
     * @return a new DistMapList which consists of the result of the operation.
     */
    public def map[S](op: (T,U)=>S): DistMapList[T,S] {
        return new DistMapList[T,S](placeGroup, team, () => {
            val dst = new HashMap[T,List[S]]();
            for (entry in data.entries()) {
                val key = entry.getKey();
                val old = entry.getValue();
                val list = new ArrayList[S](old.size());
                for (v in old) {
                    list.add(op(key, v));
                }
                dst(entry.getKey()) = list;
            }
            return dst;
        });
    }

    /**
     * Reduce the all local elements using given function.
     *
     * @param op the operation.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public def reduceLocal[S](op: (S,U)=>S, unit: S): S {
        var accum: S = unit;
        for (entry in data.entries()) {
            for (value in entry.getValue()) {
                accum = op(accum, value);
            }
        }
        return accum;
    }

}
