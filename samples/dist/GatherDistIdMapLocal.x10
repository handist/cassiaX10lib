package samples.dist;

import x10.util.*;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistIdMapLocal[U] {U haszero} {

    public val distIdMap: DistIdMap[U];

    public val gatheredCollection: HashMap[Int, HashMap[Long, U]];
    public val gatheredDist: HashMap[Int, HashMap[Long, Place]];
    public val gatheredDiff: HashMap[Int, HashMap[Long, Int]];

    public val initValues: HashMap[Long, U];
    public val initKeys: ArrayList[Long];
    public val initKeyLocation: HashMap[Long, Int];

    public val allValues: HashMap[Long, U];
    public val allKeys: ArrayList[Long];
    public val allKeyLocation: HashMap[Long, Int];

    public def this(distIdMap: DistIdMap[U]) {
        this.distIdMap = distIdMap;

	gatheredCollection = new HashMap[Int, HashMap[Long, U]]();
	gatheredDist = new HashMap[Int, HashMap[Long, Place]]();
	gatheredDiff = new HashMap[Int, HashMap[Long, Int]]();

	initValues = new HashMap[Long, U]();
	initKeys = new ArrayList[Long]();
	initKeyLocation = new HashMap[Long, Int]();

	allValues = new HashMap[Long, U]();
	allKeys = new ArrayList[Long]();
	allKeyLocation = new HashMap[Long, Int]();
    }

    public def getDistIdMap(): DistIdMap[U] = distIdMap;

    public def clearCollection() {
        gatheredCollection.clear();
    }

    public def clearDist() {
	gatheredDist.clear();
    }

    public def clearDiff() {
        gatheredDiff.clear();
    }
}
