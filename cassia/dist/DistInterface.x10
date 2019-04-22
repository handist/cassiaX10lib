package cassia.dist;

interface DistInterface[T] {
    public def cast(src:Any) : T;
    public def integrate(src:T) : void;
    public def getLocalData() : T;
    public def placeGroup() : PlaceGroup;
    public def clear() : void;

}
