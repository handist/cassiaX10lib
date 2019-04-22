package cassia.dist;

import x10.util.Pair;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.HashMap;

public class RangedDistributionLong implements RangedDistribution[LongRange] {

    private val dist: HashMap[LongRange, Place];

    public def this(distribution: RangedDistributionLong) {
        dist = cloneHashMap(distribution.getHashMap());
    }

    public def this(originalHashMap: HashMap[LongRange, Place]) {
        dist = cloneHashMap(originalHashMap);
    }

    public def clone() {
        return new RangedDistributionLong(this);
    }

    private def cloneHashMap(originalHashMap: HashMap[LongRange, Place]) {
        val newHashMap = new HashMap[LongRange, Place]();
	for (entry in originalHashMap.entries()) {
	    newHashMap.put(entry.getKey(), entry.getValue());
	}
	return newHashMap;
    }

    public def placeRanges(range: LongRange): List[Pair[Place, LongRange]] {
	val listPlaceRange = new ArrayList[Pair[Place, LongRange]]();
	for (mappedRange in dist.keySet()) {
	    val mappedPlace = dist(mappedRange);
	    if (mappedRange.min <= range.min) {
	        if (range.min <= mappedRange.max) {
		    if (range.max <= mappedRange.max) {
		        listPlaceRange.add(Pair[Place, LongRange](mappedPlace, range));
		    } else {
			listPlaceRange.add(Pair[Place, LongRange](mappedPlace, range.min..mappedRange.max));
		    }
		}
	    } else {
		if (mappedRange.min <= range.max) {
		    if (range.max <= mappedRange.max) {
		       listPlaceRange.add(Pair[Place, LongRange](mappedPlace, mappedRange.min..range.max));
		    } else {
		        listPlaceRange.add(Pair[Place, LongRange](mappedPlace, mappedRange));
		    }
	        }
	    }
	}
        return listPlaceRange;
    }

    public def getHashMap(): HashMap[LongRange, Place] {
        return dist;
    }
}
