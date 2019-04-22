package cassia.dist;

import x10.util.HashMap;

interface ManagedDistribution[K] {
    public def getDist(): HashMap[K, Place];
}