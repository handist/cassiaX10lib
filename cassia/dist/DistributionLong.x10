package cassia.dist;

import x10.util.HashMap;

public class DistributionLong implements Distribution[Long] {

        private val dist: HashMap[Long, Place];

        public def this(distribution: DistributionLong) {
	    dist = cloneHashMap(distribution.getHashMap());
	}

	public def this(originalHashMap: HashMap[Long, Place]) {
	    dist = cloneHashMap(originalHashMap);
	}

	public def this(rangedHashMap: HashMap[LongRange, Place]) {
	    val newHashMap = new HashMap[Long, Place]();
	    for (entry in rangedHashMap.entries()) {
	        val range = entry.getKey();
		val place = entry.getValue();
		for (i in range) {
		    newHashMap(i) = place;
		}
	    }
	    dist = newHashMap;
	}

	public def clone() {
	    return new DistributionLong(this);
	}

	private def cloneHashMap(originalHashMap: HashMap[Long, Place]) {
	    val newHashMap = new HashMap[Long, Place]();
	    for (entry in originalHashMap.entries()) {
	        newHashMap.put(entry.getKey(), entry.getValue());
	    }
	    return newHashMap;
	}

        public def place(key: Long): Place {
	    return dist(key);
	}

	public def getHashMap() {
	    return dist;
	}
}
