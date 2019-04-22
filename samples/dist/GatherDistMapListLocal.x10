package samples.dist;

import x10.util.*;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistMapListLocal[T, U] {T <: Comparable[T], U haszero} {

    public val distMapList: DistMapList[T, U];

    public val gatheredCollection: HashMap[Int, HashMap[T, ArrayList[U]]];
//    public val gatheredDist: HashMap[Int, HashMap[T, Place]];

    public val initValues: HashMap[T, ArrayList[U]];
    public val initKeys: ArrayList[T];
    public val initKeyValueLocation: HashMap[Pair[T, U], Int];

    public val allValues: HashMap[T, ArrayList[U]];
    public val allKeys: ArrayList[T];
    public val allKeyValueLocation: HashMap[Pair[T, U], Int];

    public def this(distMapList: DistMapList[T, U]) {
        this.distMapList = distMapList;

	gatheredCollection = new HashMap[Int, HashMap[T, ArrayList[U]]]();
//	gatheredDist = new HashMap[Int, HashMap[T, Place]]();

	initValues = new HashMap[T, ArrayList[U]]();
	initKeys = new ArrayList[T]();
	initKeyValueLocation = new HashMap[Pair[T, U], Int]();

	allValues = new HashMap[T, ArrayList[U]]();
	allKeys = new ArrayList[T]();
	allKeyValueLocation = new HashMap[Pair[T, U], Int]();
    }

    public def getDistMapList(): DistMapList[T, U] = distMapList;

    public def clearCollection() {
        gatheredCollection.clear();
    }

/*
    public def clearDist() {
	gatheredDist.clear();
    }
*/
}
