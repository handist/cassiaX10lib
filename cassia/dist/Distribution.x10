package cassia.dist;
/* 全要素を列挙できるタイプの Distribution, IterableDistribution などに改名すべ?　*/

public interface Distribution[K] /* implements Map[K,Place] */{

    public def place(key:K): Place;

    /**
     * Apply the given function to the elements(keys) of the distribuiton.
     *
     * @param func defines the behavior for the geven key:K and its location p: Place.
     */
    //public map(func: (key:K, p: Place) => void): void; 

    /**
     * Apply the given function to the elements(keys) that should be assigned to the specifiedplace
     *
     * @param place the destination place
     * @param func defines the behavior for the geven key:K
     */
    //public map(place:Place, func: (key:K) => void): void; 
}
