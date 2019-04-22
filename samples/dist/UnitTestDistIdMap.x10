package samples.dist;

//import cassia.concurrent.Pool;
import x10.util.*;
import x10.xrx.*;
import x10.lang.Math;
import cassia.dist.*;
import cassia.util.*;

public class UnitTestDistIdMap {

    private static val NPLACES: Long = Place.numPlaces();
//    private static val NTHREADS: Long = Runtime.NTHREADS as Long;

    val placeGroup: PlaceGroup;
//    val home = PlaceGroup(0): PlaceGroup;
    val team: Team;

    val numData: Long = 200;

    val distIdMap: DistIdMap[String];
    val random: Random;

    def this(placeGroup: PlaceGroup, team: Team) {
        this.placeGroup = placeGroup;
	this.team = team;
        this.random = new Random(12345);
	distIdMap = new DistIdMap[String](placeGroup, team);
    }

    public static def main(args: Rail[String]): void {
	val pg = Place.places();
	val team = new x10.util.Team(pg);
        new UnitTestDistIdMap(pg, team).run();
    }


    public def genRandStr(header: String): String {
        val rand = random.nextLong();
	return header + rand.toString();
    }

    public def init(): void {
        Place.places().broadcastFlat(() => {
            distIdMap.clear();
        });

        // Create initial data at Place 0
        //Console.OUT.println("### Create initial data at Place 0");

	for (i in 0..(numData - 1)) {
	    distIdMap(i) = genRandStr("v");
	}

	val gather = new GatherDistIdMap[String](placeGroup, distIdMap);
	gather.gather();
	gather.print();
	gather.setCurrentAsInit();

	// ---------------------------------------------------------------------------

	        // Distribute all entries
        //Console.OUT.println("");
        //Console.OUT.println("### MoveAtSync // Distribute all entries");
        Place.places().broadcastFlat(() => {
	    val mm = new MoveManagerLocal(placeGroup, team);
	    distIdMap.each((key: Long, value: String) => {
		val d = key % NPLACES;
//		Console.OUT.println("" + here + " moves key: " + key + " to " + d);
		distIdMap.moveAtSync(key, placeGroup(d), mm);
	    });

            mm.sync();
        });

	gather.gather();
	gather.print();
	if (gather.validate() &&
	    gather.validateLocationAndValue((key: Long, pid: Int) => {
		val d = key % NPLACES;
		return d as Int;
	    })) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}

        //Console.OUT.println("");
        //Console.OUT.println("### Update dist // Distribute all entries");
	placeGroup.broadcastFlat(() => {
	    distIdMap.updateDist();
	});

	gather.gather();
	gather.print();
	if (gather.validate() && gather.validateAfterUpdateDist()) {
	    Console.OUT.println("VALIDATE: SUCCESS");
	} else {
	    Console.OUT.println("VALIDATE: FAIL");
	}

	Console.OUT.println("init() done");
    }

    public def add01(): boolean {
        init();

	val key = numData;

	try {
            placeGroup.broadcastFlat(() => {
		distIdMap(key) = genRandStr("v");
		if (!distIdMap.containsKey(key) ||
		    distIdMap.getDist()(key) != here ||
		    distIdMap.getDiff()(key) != DistManager.DIST_ADDED) {
		    throw new Exception("UnitTestDistIdMap#add01()");
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;	
    }

    public def add02(): boolean {
        init();

        val newKey = numData;

	try {
	    placeGroup.broadcastFlat(() => {
	        distIdMap(newKey) = genRandStr("v");
	        distIdMap.updateDist();
	    });
            throw new Exception("UnitTestDistIdMap#add02()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != DistManager.DIST_ADDED) {
		        return false;
		    } else {
//                	Console.OUT.println("Exception");
		    }
		} else {
		    return false;
		}
	    }
	}

	return true;
    }

    public def add03(): boolean {
        init();

        val removeKey = numData / 2 + 1;
	val newVal = genRandStr("v");

	try {
	    placeGroup.broadcastFlat(() => {
	        if (distIdMap.containsKey(removeKey)) {
	            distIdMap.remove(removeKey);
		    distIdMap(removeKey) = newVal;
		    if (!distIdMap.containsKey(removeKey) ||
		        distIdMap(removeKey) != newVal ||
		        distIdMap.getDiff().containsKey(removeKey) ||
			distIdMap.getDist()(removeKey) != here) {
		        throw new Exception("UnitTestDistIdMap#add03()");
		    }
	            distIdMap.updateDist();
	        } else {
	            distIdMap.updateDist();
		}
	    });
        } catch (e: Exception) {
	    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
            Console.OUT.println(msg);
	    return false;
	}

	return true;	
    }

    public def put01(): boolean {
        init();

	try {
	    placeGroup.broadcastFlat(() => {
	        val valueList = new ArrayList[String]();
		distIdMap.each((key: Long, value: String) => {
		    val newVal = genRandStr("v");
		    if ((distIdMap(key) = newVal).equals(newVal)) {
		        throw new Exception("UnitTestDistIdMap#put01() A");
		    };
		    valueList.add(newVal);
		});
		val valueListIt = valueList.iterator();
		distIdMap.each((key: Long, value: String) => {
		    if (!distIdMap(key).equals(valueListIt.next()) ||
		        distIdMap.getDiff().containsKey(key)) {
		        throw new Exception("UnitTestDistIdMap#put01() B");
		    }
		});
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
                Console.OUT.println(msg);
	        return false;
	    }
	}

	return true;
    }

    public def put02(): boolean {
        init();

	val key = numData;
	try {
	    placeGroup.broadcastFlat(() => {
	        val oldData = genRandStr("v");
		val newData = genRandStr("v");
	        distIdMap(key) = oldData;
		if (!(distIdMap(key) = newData).equals(oldData)) {
		    throw new Exception("UnitTestDistIdMap#put02() A");
		}
		if (!distIdMap(key).equals(newData) ||
		    distIdMap.getDiff()(key) != DistManager.DIST_ADDED ||
		    distIdMap.getDist()(key) != here) {
		    throw new Exception("UnitTestDistIdMap#put02() B");
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
                Console.OUT.println(msg);
	        return false;
	    }
	}

	return true;
    }

    public def put03(): boolean {
        init();

	try {
	    placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        distIdMap.each((key: Long, value: String) => {
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();

	        val valueList = new ArrayList[String]();
		distIdMap.each((key: Long, value: String) => {
		    val newVal = genRandStr("v");
		    if ((distIdMap(key) = newVal).equals(newVal) ||
		        distIdMap.getDiff()(key) != DistManager.DIST_MOVED_IN) {
		        throw new Exception("UnitTestDistIdMap#put03() A");
		    };
		    valueList.add(newVal);
		});
		val valueListIt = valueList.iterator();
		distIdMap.each((key: Long, value: String) => {
		    if (!distIdMap(key).equals(valueListIt.next()) ||
		        distIdMap.getDiff()(key) != DistManager.DIST_MOVED_IN) {
		        throw new Exception("UnitTestDistIdMap#put03() B");
		    }
		});
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
                Console.OUT.println(msg);
	        return false;
	    }
	}

	return true;
    }


/*    
    public def add05(): boolean {
        init();

	val existKey = numData / 2;
	val newVal = genRandStr("v");
	
	try {
            placeGroup.broadcastFlat(() => {
	        if (distIdMap.containsKey(existKey)) {
		    if (distIdMap.getDiff().containsKey(existKey)) {
		        throw new Exception("UnitTestDistIdMap#add05()");
		    }
		    distIdMap(existKey) = newVal;
		}
	    });
            throw new Exception("UnitTestDistIdMap#add05()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 102N) {
		        return false;
		    }
		} else {
		    return false;
		}
	    }
	}

	return true;
    }

    public def add06(): boolean {
        init();

	val existKey1 = numData;
	val existKey2 = numData / 2 + 1;
	val newVal1 = genRandStr("v");
	val newVal2 = genRandStr("v");

	try {
            placeGroup.broadcastFlat(() => {
	        if (here.id == 0) {
		    distIdMap(existKey1) = newVal1;
		    if (distIdMap.getDiff()(existKey1) != DistManager.DIST_ADDED) {
		        throw new Exception("UnitTestDistIdMap#add06()");
		    }
		    distIdMap(existKey1) = newVal2;
		}	        
	    });
            throw new Exception("UnitTestDistIdMap#add06()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 103N) {
		        return false;
		    }
		} else {
		    return false;
		}
	    }
	}

	init();

	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        distIdMap.each((key: Long, value: String) => {
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();

	        if (distIdMap.containsKey(existKey2)) {
		    if (distIdMap.getDiff()(existKey2) != DistManager.DIST_MOVED_IN) {
		        throw new Exception("UnitTestDistIdMap#add06()");
		    }
		    distIdMap(existKey2) = newVal1;
		}
	    });
            throw new Exception("UnitTestDistIdMap#add06()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 103N) {
		        return false;
		    }
		} else {
		    return false;
		}
	    }
	}

	return true;
    }
*/

    public def add08(): boolean {
        init();

	val existKey = numData;
	val newVal = genRandStr("v");
	
	try {
            placeGroup.broadcastFlat(() => {
	        if (here.id == 0) {
		    val ldist = distIdMap.getDist();
		    for (entry in ldist.entries()) {
		        if (entry.getValue() != here) {
			    distIdMap(entry.getKey()) = newVal;
			}
		    }
		}	        
	    });
            throw new Exception("UnitTestDistIdMap#add08()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 105N) {
		        return false;
		    }
		} else {
		    return false;
		}
	    }
	}

	return true;
    }


    public def remove01(): boolean {
        init();

        val newKey = numData;

	try {
	    placeGroup.broadcastFlat(() => {
	        if (!distIdMap.containsKey(newKey)) {
	            distIdMap.remove(newKey);
                }
	    });   
	    throw new Exception("UnitTestDistIdMap#remove01()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 201N) {
		        return false;
		    }
		} else {
		    return false;
		}
	    }
	}

	return true;
    }

    public def remove02(): boolean {
        init();

	try {
	    placeGroup.broadcastFlat(() => {
		val keyList = new ArrayList[Long]();
		distIdMap.each((key: Long, value: String) => {
		    keyList.add(key);
		});
		for (key in keyList) {
		    if (distIdMap.getDiff().containsKey(key)) {
		        throw new Exception("UnitTestDistIdMap#remove02()");
		    }
		    distIdMap.remove(key);
		    if (distIdMap.getDiff()(key) != DistManager.DIST_REMOVED ||
		        distIdMap.getDist().containsKey(key)) {
		        throw new Exception("UnitTestDistIdMap#remove02()");
		    }
		}
	    });   
	} catch (e: Exception) {
	    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
            Console.OUT.println(msg);
	    return false;
	}

	return true;
    }

    public def remove03(): boolean {
        init();

        val key = numData;

	try {
	    placeGroup.broadcastFlat(() => {
	        if (distIdMap.containsKey(key - 1)) {
		    distIdMap(key) = genRandStr("v");
		    if (distIdMap.getDiff()(key) != DistManager.DIST_ADDED) {
		        throw new Exception("UnitTestDistIdMap#remove03()");
		    }
		    distIdMap.remove(key);
		    if (distIdMap.getDiff().containsKey(key) ||
		        distIdMap.getDist().containsKey(key)) {
		        throw new Exception("UnitTestDistIdMap#remove03()");
		    }
		}
	    });   
	} catch (e: Exception) {
	    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
            Console.OUT.println(msg);
	    return false;
	}

	return true;
    }

    public def remove04(): boolean {
        init();

	try {
	    placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        distIdMap.each((key: Long, value: String) => {
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();
		val keyList = new ArrayList[Long]();
		distIdMap.each((key: Long, value: String) => {
		    keyList.add(key);
		});
		for (key in keyList) {
		    if (distIdMap.getDiff()(key) != DistManager.DIST_MOVED_IN) {
		        throw new Exception("UnitTestDistIdMap#remove04()");
		    }
		    distIdMap.remove(key);
		    if (distIdMap.getDiff()(key) != DistManager.DIST_REMOVED ||
		        distIdMap.getDist().containsKey(key)) {
		        throw new Exception("UnitTestDistIdMap#remove04()");
		    }
		}
	    });
	} catch (e: Exception) {
	    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
            Console.OUT.println(msg);
	    return false;
	}

	return true;
    }

    public def remove06(): boolean {
        init();

	try {
	    placeGroup.broadcastFlat(() => {
	        val keyList = new ArrayList[Long]();
	        for (entry in distIdMap.getDist().entries()) {
		    if (entry.getValue() != here) {
		        val key = entry.getKey();
			keyList.add(key);
		    }
		}
		for (key in keyList) {
		    distIdMap.remove(key);
		}
		throw new Exception("UnitTestDistIdMap#remove06()");
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 203N) {
		        return false;
		    }
		} else {
	    	    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
		    Console.OUT.println(msg);
	    	    return false;
		}
	    }
	}

	return true;
    }

    public def moveout01(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
		distIdMap.moveAtSync(key, destination, mm);
		mm.sync();
                throw new Exception("UnitTestDistIdMap#moveout01() A");
	    });
            throw new Exception("UnitTestDistIdMap#moveout01()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 801N) {
		        return false;
		    }
		} else {
		    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	    Console.OUT.println(msg);
		    return false;
		}
	    }
	}

	return true;
    }

    public def moveout02(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        val keyList = new ArrayList[Long]();
	        for (entry in distIdMap.getDist().entries()) {
		    if (entry.getValue() == here) {
		        val key = entry.getKey();
			keyList.add(key);
		    }
		}
		for (key in keyList) {
		    distIdMap.remove(key);
		}
		for (key in keyList) {
		    if (distIdMap.containsKey(key) ||
		        distIdMap.getDiff()(key) != DistManager.DIST_REMOVED) {
		        throw new Exception("UnitTestDistIdMap#moveout02() A " + distIdMap.getDiff()(key));
		    }
		    distIdMap.moveAtSync(key, destination, mm);
                }
		mm.sync();
		throw new Exception("UnitTestDistIdMap#moveout02() B");
	    });
            throw new Exception("UnitTestDistIdMap#moveout02()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 802N) {
		        return false;
		    }
		} else {
		    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	    Console.OUT.println(msg);
		    return false;
		}
	    }
	}

	return true;
    }

    public def moveout04(): boolean {
        init();

	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        val keyList = new ArrayList[Long]();
	        for (entry in distIdMap.getDist().entries()) {
		    if (entry.getValue() == here) {
		        val key = entry.getKey();
			keyList.add(key);
		    }
		}
		for (key in keyList) {
		    distIdMap.moveAtSync(key, destination, mm);
                }
		mm.sync();
		for (key in keyList) {
		    if (distIdMap.containsKey(key) ||
		        distIdMap.getDiff().containsKey(key) ||
			distIdMap.getDist()(key) != destination) {
		        throw new Exception("UnitTestDistIdMap#moveout04()");
  		    }
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def moveout05(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
		distIdMap(key + here.id) = genRandStr("v");
		distIdMap.moveAtSync(key + here.id, destination, mm);
		mm.sync();
		if (distIdMap.containsKey(key + here.id) ||
		    distIdMap.getDiff().containsKey(key + here.id) ||
		    distIdMap.getDist().containsKey(key + here.id)) {
		    throw new Exception("UnitTestDistIdMap#moveout05()");
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def moveout06(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        distIdMap.each((key: Long, value: String) => {
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();
		val keyList = new ArrayList[Long]();
		distIdMap.each((key: Long, value: String) => {
		    keyList.add(key);
		});
		for (key in keyList) {
		    if (distIdMap.getDiff()(key) != DistManager.DIST_MOVED_IN) {
		        throw new Exception("UnitTestDistIdMap#moveout06() A");
		    }
		    distIdMap.moveAtSync(key, destination, mm);
		}
		mm.sync();
		for (key in keyList) {
		    if (distIdMap.containsKey(key) ||
		        distIdMap.getDiff().containsKey(key) ||
		        distIdMap.getDist()(key) != destination) {
		        throw new Exception("UnitTestDistIdMap#moveout06() B");
		    }
		}		
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def moveout08(): boolean {
        init();
	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        val keyList = new ArrayList[Long]();
	        for (entry in distIdMap.getDist().entries()) {
		    if (entry.getValue() != here) {
		        val key = entry.getKey();
			keyList.add(key);
		    }
		}
		for (key in keyList) {
		    distIdMap.moveAtSync(key, destination, mm);
                }
		mm.sync();
		throw new Exception("UnitTestDistIdMap#moveout08() B");
	    });
            throw new Exception("UnitTestDistIdMap#moveout08()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 805N) {
		        return false;
		    }
		} else {
		    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	    Console.OUT.println(msg);
		    return false;
		}
	    }
	}

	return true;
    }

    public def movein01(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
		distIdMap(key + here.id) = genRandStr("v");
		distIdMap.moveAtSync(key + here.id, destination, mm);
		mm.sync();
		val origin = (here.id == 0) ? NPLACES - 1 : here.id - 1;
		
		if (!distIdMap.containsKey(key + origin) ||
		    distIdMap.getDiff()(key + origin) != DistManager.DIST_ADDED ||
		    distIdMap.getDist()(key + origin) != here) {
		    throw new Exception("UnitTestDistIdMap#movein01()");
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def movein03(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
		for (i in 0..(NPLACES - 1)) {
		    distIdMap(key + i) = genRandStr("v");
		}
		distIdMap.moveAtSync(key + destination.id, destination, mm);
		mm.sync();
		throw new Exception("UnitTestDistIdMap#movein03() B");
	    });
            throw new Exception("UnitTestDistIdMap#movein03()");
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
	        if (e instanceof DistManager.ParameterErrorException) {
		    if ((e as DistManager.ParameterErrorException).reason != 402N) {
		        return false;
		    }
		} else {
		    val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	    Console.OUT.println(msg);
		    return false;
		}
	    }
	}

	return true;
    }

    public def movein05(): boolean {
        init();

	val key = numData;
	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
		distIdMap(key + here.id) = genRandStr("v");
		for (i in 0..(NPLACES - 1)) {
		    val k = (here.id - i + NPLACES) % NPLACES;
		    distIdMap.moveAtSync(key + k, destination, mm);
		    mm.sync();
		}
		if (!distIdMap.containsKey(key + here.id) ||
		    distIdMap.getDiff()(key + here.id) != DistManager.DIST_ADDED ||
		    distIdMap.getDist()(key + here.id) != here) {
       		    throw new Exception("UnitTestDistIdMap#movein05()");
		}
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def movein09(): boolean {
        init();

	try {
            placeGroup.broadcastFlat(() => {
 	        val mm = new MoveManagerLocal(placeGroup, team);
	        val destination = Place.places().next(here);
	        distIdMap.each((key: Long, value: String) => {
		    distIdMap.moveAtSync(key, destination, mm);
	        });
                mm.sync();
	        distIdMap.each((key: Long, value: String) => {
		    if (distIdMap.getDiff()(key) != DistManager.DIST_MOVED_IN ||
		        distIdMap.getDist()(key) != here) {
            		throw new Exception("UnitTestDistIdMap#movein09()");
		    }
	        });
	    });
	} catch (me: MultipleExceptions) {
	    for (e in me.exceptions) {
		val msg = "[" + here.id + "] Uncaught exception: " + e.toString();
        	Console.OUT.println(msg);
		return false;
	    }
	}

	return true;
    }

    public def run(): void {

	if (add01()) {
	    Console.OUT.println("### add01: SUCCESS");
	} else {
	    Console.OUT.println("### add01: FAIL");
	}

	if (add02()) {
	    Console.OUT.println("### add02: SUCCESS");
	} else {
	    Console.OUT.println("### add02: FAIL");
	}

	if (add03()) {
	    Console.OUT.println("### add03: SUCCESS");
	} else {
	    Console.OUT.println("### add03: FAIL");
	}

/*
	if (add05()) {
	    Console.OUT.println("### add05: SUCCESS");
	} else {
	    Console.OUT.println("### add05: FAIL");
	}

	if (add06()) {
	    Console.OUT.println("### add06: SUCCESS");
	} else {
	    Console.OUT.println("### add06: FAIL");
	}
*/

	if (add08()) {
	    Console.OUT.println("### add08: SUCCESS");
	} else {
	    Console.OUT.println("### add08: FAIL");
	}

	if (put01()) {
	    Console.OUT.println("### put01: SUCCESS");
	} else {
	    Console.OUT.println("### put01: FAIL");
	}

	if (put02()) {
	    Console.OUT.println("### put02: SUCCESS");
	} else {
	    Console.OUT.println("### put02: FAIL");
	}

	if (put03()) {
	    Console.OUT.println("### put03: SUCCESS");
	} else {
	    Console.OUT.println("### put03: FAIL");
	}

	if (remove01()) {
	    Console.OUT.println("### remove01: SUCCESS");
	} else {
	    Console.OUT.println("### remove01: FAIL");
	}

	if (remove02()) {
	    Console.OUT.println("### remove02: SUCCESS");
	} else {
	    Console.OUT.println("### remove02: FAIL");
	}

	if (remove03()) {
	    Console.OUT.println("### remove03: SUCCESS");
	} else {
	    Console.OUT.println("### remove03: FAIL");
	}

	if (remove04()) {
	    Console.OUT.println("### remove04: SUCCESS");
	} else {
	    Console.OUT.println("### remove04: FAIL");
	}

	if (remove06()) {
	    Console.OUT.println("### remove06: SUCCESS");
	} else {
	    Console.OUT.println("### remove06: FAIL");
	}

	if (moveout01()) {
	    Console.OUT.println("### moveout01: SUCCESS");
	} else {
	    Console.OUT.println("### moveout01: FAIL");
	}

	if (moveout02()) {
	    Console.OUT.println("### moveout02: SUCCESS");
	} else {
	    Console.OUT.println("### moveout02: FAIL");
	}

	if (moveout04()) {
	    Console.OUT.println("### moveout04: SUCCESS");
	} else {
	    Console.OUT.println("### moveout04: FAIL");
	}

	if (moveout05()) {
	    Console.OUT.println("### moveout05: SUCCESS");
	} else {
	    Console.OUT.println("### moveout05: FAIL");
	}

	if (moveout06()) {
	    Console.OUT.println("### moveout06: SUCCESS");
	} else {
	    Console.OUT.println("### moveout06: FAIL");
	}

	if (moveout08()) {
	    Console.OUT.println("### moveout08: SUCCESS");
	} else {
	    Console.OUT.println("### moveout08: FAIL");
	}

	if (movein01()) {
	    Console.OUT.println("### movein01: SUCCESS");
	} else {
	    Console.OUT.println("### movein01: FAIL");
	}

	if (movein03()) {
	    Console.OUT.println("### movein03: SUCCESS");
	} else {
	    Console.OUT.println("### movein03: FAIL");
	}

	if (movein05()) {
	    Console.OUT.println("### movein05: SUCCESS");
	} else {
	    Console.OUT.println("### movein05: FAIL");
	}

	if (movein09()) {
	    Console.OUT.println("### movein09: SUCCESS");
	} else {
	    Console.OUT.println("### movein09: FAIL");
	}

    }
}
