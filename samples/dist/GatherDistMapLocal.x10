package samples.dist;

import x10.util.*;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistMapLocal[T, U] {T <: Comparable[T], U haszero} {

    public val distMap: DistMap[T, U];

    public val gatheredCollection: HashMap[Int, HashMap[T, U]];
//    public val gatheredDist: HashMap[Int, HashMap[T, Place]];

    public val initValues: HashMap[T, U];
    public val initKeys: ArrayList[T];
    public val initKeyLocation: HashMap[T, Int];

    public val allValues: HashMap[T, U];
    public val allKeys: ArrayList[T];
    public val allKeyLocation: HashMap[T, Int];

    public def this(distMap: DistMap[T, U]) {
        this.distMap = distMap;

	gatheredCollection = new HashMap[Int, HashMap[T, U]]();
//	gatheredDist = new HashMap[Int, HashMap[T, Place]]();

	initValues = new HashMap[T, U]();
	initKeys = new ArrayList[T]();
	initKeyLocation = new HashMap[T, Int]();

	allValues = new HashMap[T, U]();
	allKeys = new ArrayList[T]();
	allKeyLocation = new HashMap[T, Int]();
    }

    public def getDistMap(): DistMap[T, U] = distMap;

    public def clearCollection() {
        gatheredCollection.clear();
    }

/*
    public def clearDist() {
	gatheredDist.clear();
    }
*/
}
