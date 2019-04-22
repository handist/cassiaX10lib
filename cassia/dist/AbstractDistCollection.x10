package cassia.dist;

import x10.compiler.NonEscaping;
import x10.compiler.TransientInitExpr;
import x10.util.concurrent.Lock;
import x10.util.HashMap;
import x10.util.Map;
import x10.util.Team;

public abstract class AbstractDistCollection[T] {

    private val localHandle : PlaceLocalHandle[Local[T]];

    @TransientInitExpr(getLocalData())
    protected transient val data: T;

    @TransientInitExpr(getPlaceGroup())
    protected transient val placeGroup: PlaceGroup;

    @TransientInitExpr(getTeam())
    protected transient val team: Team;

    @TransientInitExpr(getLock())
    private transient val lock: Lock;

    protected def this(placeGroup: PlaceGroup, team: Team, init: ()=>T) {
        this(placeGroup, (): Local[T] => {
            val data = init();
            return new Local[T](placeGroup, team, data);
        });
    }

    protected def this(placeGroup: PlaceGroup, init: ()=>Local[T]) {
        val plh = PlaceLocalHandle.makeFlat[Local[T]](placeGroup, init);
        this.localHandle = plh;
        this.data = getLocalData();
        this.placeGroup = getPlaceGroup();
        this.team = getTeam();
        this.lock = getLock();
    }

    @NonEscaping final protected def getLocal[S]() {S <: Local[T]}: S {
        return (localHandle() as S);
    }

    // for internal use
    @NonEscaping final protected def getLocalData(): T {
        val local = this.localHandle();
        return local.data;
    }

    // for internal use
    @NonEscaping final protected def getPlaceGroup(): PlaceGroup {
        val local = this.localHandle();
        if (local == null) {
            return Zero.get[PlaceGroup]();
        }
        return local.placeGroup;
    }

    // for internal use
    @NonEscaping final protected def getTeam(): Team {
        val local = this.localHandle();
        if (local == null) {
            return Zero.get[Team]();
        }
        return local.team;
    }

    // for internal use
    @NonEscaping final protected def getLock(): Lock {
        val local = this.localHandle();
        if (local == null) {
            return new Lock();
        }
        return local.lock;
    }

    /**
     * Return the PlaceGroup.
     *
     * @return PlaceGroup.
     */
    public def placeGroup(): PlaceGroup = placeGroup;

    /**
     * Return the Team.
     *
     * @return Team.
     */
    public def team(): Team = team;

    def localData(): T = data;

    public def cast(src:Any) : T{
        return src as T;
    }

    public abstract def clear() : void;

    public abstract def integrate(src : T) : void;

    public abstract def balance() : void;

    /**
     * Destroy an instance of AbstractDistCollection.
     */
    public def destroy(): void {
        PlaceLocalHandle.destroy(placeGroup, localHandle);
    }

    protected def lock(): void {
        lock.lock();
    }

    protected def unlock(): void {
        lock.unlock();
    }


    public final def printAllData(){
        for(p in placeGroup){
            at(p){
                printLocalData();
            }
        }
    }

    public def printLocalData(){
        Console.OUT.println(data);
    }
}
