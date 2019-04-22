package samples.dist;

import x10.util.*;
import cassia.dist.*;
import cassia.util.*;

public class GatherDistColLocal[T] {T haszero} {

    public val distCol: DistCol[T];

    public val gatheredCollection: HashMap[Int, HashMap[LongRange, Rail[T]]];
    public val gatheredDist: HashMap[Int, HashMap[LongRange, Place]];
    public val gatheredDiff: HashMap[Int, HashMap[LongRange, Int]];

    public val initValues: HashMap[Long, T];
    public val initKeys: ArrayList[Long];
    public val initKeyLocation: HashMap[Long, Int];

    public val allValues: HashMap[Long, T];
    public val allKeys: ArrayList[Long];
    public val allKeyLocation: HashMap[Long, Int];

    public def this(distCol: DistCol[T]) {
        this.distCol = distCol;

	gatheredCollection = new HashMap[Int, HashMap[LongRange, Rail[T]]]();
	gatheredDist = new HashMap[Int, HashMap[LongRange, Place]]();
	gatheredDiff = new HashMap[Int, HashMap[LongRange, Int]]();

	initValues = new HashMap[Long, T]();
	initKeys = new ArrayList[Long]();
	initKeyLocation = new HashMap[Long, Int]();

	allValues = new HashMap[Long, T]();	
	allKeys = new ArrayList[Long]();
	allKeyLocation = new HashMap[Long, Int]();
    }

    public def getDistCol(): DistCol[T] = distCol;

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
