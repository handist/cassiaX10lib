package cassia.dist;

import x10.util.Indexed;
import x10.util.List;
import x10.util.ArrayList;
import x10.util.Map;
import x10.util.HashMap;
import x10.util.RailBuilder;
import x10.util.RailUtils;
import x10.util.Team;
import x10.io.Serializer;
import x10.io.Deserializer;



/**
 * This class is used for relocating elements of DistCollections.
 */
public final class MoveManagerLocal {

    val placeGroup: PlaceGroup;
    val team: Team;
    val serializeListMap: Map[Place, List[(Serializer)=>void]];
    val builders: Map[Place, RailBuilder[(Deserializer)=>void]];


    /**
     * Construct a MoveManagerLocal with given arguments.
     *
     * @param placeGroup PlaceGroup.
     * @param team Team
     */
    public def this(placeGroup: PlaceGroup, team: Team) {
	this.placeGroup = placeGroup;
	this.team = team;
        serializeListMap = new HashMap[Place,List[(Serializer)=>void]](placeGroup.size());
        builders = new HashMap[Place,RailBuilder[(Deserializer)=>void]](placeGroup.size());
        for (place in placeGroup) {
            serializeListMap(place) = new ArrayList[(Serializer)=>void]();
            builders(place) = new RailBuilder[(Deserializer)=>void]();
        }
    }

    public def request(pl: Place, serialize: (Serializer)=>void, deserialize: (Deserializer)=>void): void {
        serializeListMap(pl).add(serialize);
        builders(pl).add(deserialize);
    }

    def clear(): void {
        for (entry in serializeListMap.entries()) {
            entry.getValue().clear();
        }
        for (entry in builders.entries()) {
            entry.setValue(new RailBuilder[(Deserializer)=>void]());
        }
    }

    /**
     * Request to reset the Serializer at the specified place.
     *
     * @param atPlace the target place.
     */
    public def reset(pl: Place): void {
        serializeListMap(pl).add((s: Serializer) => {
            s.newObjectGraph();
        });
    }

    public def executeSerialization(place:Place) throws Exception :Rail[Byte] {
	val s = new Serializer();
	s.writeAny(builders(place).result());
	val serializeList = serializeListMap(place);
	for (i in 0 .. (serializeList.size() - 1)) {
	    val serialize = serializeList(i);
	    serialize(s);
	}
	return s.toRail();
    }

    public def executeDeserialization(map: Map[Place, Rail[Byte]]) throws Exception :void {
        for (place in placeGroup) {
            val ds = new Deserializer(map(place));
            val deserializeList = ds.readAny() as Rail[(Deserializer)=>void];
            for (i in deserializeList.range()) {
                val deserialize = deserializeList(i);
                deserialize(ds);
            }
        }
    }

    /**
     * Execute the all requests synchronously.
     */
    public def sync() throws Exception {
        CollectiveRelocator.all2allser(placeGroup, team, this);
    }

    /* 将来的に
      moveAtSync(dist:RangedDistribution, mm) を 持つものを interface 宣言するのかな？
      public def moveAssociativeCollectionsAtSync(dist: RangedDistribution, dists: List[RangedMoballe]) {

      }
    public def moveAssosicativeCollectionsAtSync(dist: Distribution[K]) {
        // add dist to the list to schedule 
    }
    */
}


