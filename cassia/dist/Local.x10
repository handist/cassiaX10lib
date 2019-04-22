package cassia.dist;
import x10.compiler.TransientInitExpr;
import x10.util.concurrent.Lock;
import x10.util.Team;

class Local[S] {

    val placeGroup : PlaceGroup;
    val team : Team;
    val data : S;

    @TransientInitExpr(new Lock())
    transient val lock: Lock = new Lock();

    def this(placeGroup: PlaceGroup, team: Team, data: S){
        this.placeGroup = placeGroup;
        this.team = team;
        this.data = data;
    }
}
