package cassia.dist;

import x10.util.List;
import x10.util.Container;
import x10.util.ArrayList;
import x10.util.Set;
import x10.util.Map;
import x10.util.HashMap;
import x10.util.HashSet;
import x10.util.Pair;
import x10.util.RailBuilder;
import x10.util.Team;
import x10.io.Serializer;
import x10.io.Deserializer;
import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;

public class DistManager[T] {

    public val dist = new HashMap[T, Place]();
    public val diff = new HashMap[T, Int]();;
    val importedDiffKeys = new HashSet[T]();

    public static DIST_ADDED: Int = 1N;
    public static DIST_REMOVED: Int = 2N;
    public static DIST_MOVED_IN: Int = 4N;

    public static MOVE_NEW: Int = 1N;
    public static MOVE_OLD: Int = 2N;
    public static MOVE_NONE: Int = 0N;

    public def distHasKey(key: T): boolean {
        return dist.containsKey(key);
    }

    public def distIsLocal(key: T): boolean {
        return dist(key) == here;
    }

    public def diffHasKey(key: T): boolean {
        return diff.containsKey(key);
    }

    public def diffOfKeyIs(key: T, operation: Int): boolean {
        return (diff(key) & operation) != 0N;
    }

    public def clear(): void {
        dist.clear();
        diff.clear();
    }

    def reject(method: String, reason: Int, key: T) throws ParameterErrorException {
        val msg = "[" + here.id + "] Error when calling " + method + " " + key + " on code " + reason;
        Console.OUT.println(msg);
	if (reason > 0N) throw new ParameterErrorException(reason, msg);
        throw new ParameterErrorException(reason, msg);
    }

    def systemError(method: String, reason: Int, key: T) throws SystemErrorException {
        val msg = "[" + here.id + "] System Error when calling " + method + " " + key + " on code " + reason;
        Console.OUT.println(msg);
	if (reason > 0N) throw new SystemErrorException(reason, msg);
	throw new SystemErrorException(reason, msg);
    }

    public def add(key: T) throws Exception {
        if (distHasKey(key)) {
	    if (distIsLocal(key)) {
	        if (diffHasKey(key)) {
		    if (diffOfKeyIs(key, (DIST_ADDED | DIST_MOVED_IN))) {
		        reject("add", 103N, key);
		    } else {
		        systemError("add", 104N, key);
		    }
		} else {
		    reject("add", 102N, key);
		}
	    } else {
	        // !distIsLocal(key)
		reject("add", 105N, key);
	    }
	} else {
	    // !distHasKey(key)
	    if (diffHasKey(key)) {
	        if (diffOfKeyIs(key, DIST_REMOVED)) {
		    diff.remove(key);
		    dist(key) = here;
		} else {
		    systemError("add", 101N, key);
		}
	    } else {
	        diff(key) = DIST_ADDED;
	        dist(key) = here;
	    }
	}  
    }

    public def remove(key: T) throws Exception {
        if (distHasKey(key)) {
	    if (distIsLocal(key)) {
	        if (diffHasKey(key)) {
//		    Console.OUT.println("[" + here.id + "] remove key " + key);
		    if (diffOfKeyIs(key, DIST_ADDED)) {
		        diff.remove(key);
			dist.remove(key);
		    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
		        diff(key) = DIST_REMOVED;
			dist.remove(key);		    
		    } else {
		        systemError("remove", 202N, key);
		    }		
		} else {
		    diff(key) = DIST_REMOVED;
		    dist.remove(key);
		}
	    } else {
	        // !distIsLocal(key)
		reject("remove", 203N, key);
	    }
	} else {
	    // !distHasKey(key)
	    reject("remove", 201N, key);
	}  
    }

    public def moveOut(key: T, dest: Place) throws Exception :int {
        if (distHasKey(key)) {
//	    Console.OUT.println(">>> distHasKey");
	    if (distIsLocal(key)) {
	        if (diffHasKey(key)) {
		    if (diffOfKeyIs(key, DIST_ADDED)) {
		        diff.remove(key);
		        dist.remove(key);
			return MOVE_NEW;
		    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
		        diff.remove(key);
		        dist(key) = dest;
			return MOVE_OLD;
		    } else {
		        systemError("moveOut", 804N, key);
		    }
		} else {
		    dist(key) = dest;
		    return MOVE_OLD;
		}
	    } else {
	        // !distIsLocal(key)
		reject("moveOut", 805N, key);
	    }
	} else {
	    // !distHasKey(key)
//	    Console.OUT.println(">>> !distHasKey");
	    if (diffHasKey(key)) {
//	        Console.OUT.println(">>> diffHasKey");
	        if (diffOfKeyIs(key, DIST_REMOVED)) {
		    reject("moveOut", 802N, key);
		} else {
		    systemError("moveOut", 803N, key);
		}
	    } else {
//	        Console.OUT.println(">>> !diffHasKey");
	        reject("moveOut", 801N, key);
	    }
	}
//	Console.OUT.println(">>> MOVE_NONE");
	return MOVE_NONE;
    }


    public def moveInNew(key: T) throws Exception {
//        Console.OUT.println(">>> moveInNew " + key + " distHasKey: " + distHasKey(key) + " diffHasKey: " + diffHasKey(key));

        if (distHasKey(key)) {
	    if (distIsLocal(key)) {
	        if (diffHasKey(key) && diffOfKeyIs(key, DIST_ADDED)) {
		    reject("moveInNew", 402N, key);
		} else {
		    systemError("moveInNew", 403N, key);		
		}
	    } else {
	        // !distIsLocal(key)
		if (diffHasKey(key)) {
		    systemError("moveInNew", 404N, key);
		} else {
//		    Console.OUT.println(">>> AAA");
		    diff(key) = DIST_ADDED;
		    dist(key) = here;
		}
	    }
	} else {
	    // !distHasKey(key)
	    if (diffHasKey(key)) {
	        systemError("moveInNew", 401N, key);
	    } else {
//		Console.OUT.println(">>> BBB");
	        diff(key) = DIST_ADDED;
		dist(key) = here;
	    }
	}  
    }

    public def moveInOld(key: T) throws Exception {
        if (distHasKey(key)) {
	    if (distIsLocal(key)) {
	        systemError("moveInOld", 406N, key);
	    } else {
	        // !distIsLocal(key)
		if (diffHasKey(key)) {
		    systemError("moveInOld", 407N, key);
		} else {
		    diff(key) = DIST_MOVED_IN;
		    dist(key) = here;
		}
	    }
	} else {
	    // !distHasKey(key)
	    systemError("moveInOld", 405N, key);
	}  
    }

    def applyDiff(key: T, operation: Int, from: Place) throws Exception {
//        Console.OUT.println("[" + here.id + "] applyDiff " + key + " op: " + operation + " from: " + from.id);
        if (importedDiffKeys.contains(key) || diff.containsKey(key)) {
	    reject("applyDiff with duplicate key ", operation, key);
	} else {
	    importedDiffKeys.add(key);
	    if ((operation & (DIST_ADDED | DIST_MOVED_IN)) != 0N) {
	        dist(key) = from;
	    } else {
	        // operation == DIST_REMOVED
	        dist.remove(key);
	    }
	}
    }

    def setup(keys: Container[T]) throws Exception {
        for (k in keys) {
	    add(k);
	}
    }

    def updateDist(pg:PlaceGroup, team:Team) throws Exception {
	val serProcess = (ser:Serializer)=> { 
	    ser.writeAny(diff); 
	};
	val desProcess = (des: Deserializer, from: Place)=> { 
	    val importedDiff = des.readAny() as Map[T, Int];
	    for(entry in importedDiff.entries()) {
		val k = entry.getKey();
		val v = entry.getValue();
		applyDiff(k, v, from);
	    }
	};
	CollectiveRelocator.allgatherSer(pg, team, serProcess, desProcess);
	importedDiffKeys.clear();	
	diff.clear();
    }

    public static class Index extends DistManager[Long] {
    }

    public static class Range extends DistManager[LongRange] {
    }

    public static class SystemErrorException extends Exception {
        public val reason: Int;

        def this(reason: Int, msg: String) {
	    super(msg);
	    this.reason = reason;
	}
    }

    public static class ParameterErrorException extends Exception {
        public val reason: Int;

        def this(reason: Int, msg: String) {
	    super(msg);
	    this.reason = reason;
	}
    }

}
