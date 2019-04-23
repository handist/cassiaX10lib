

# Cassia X10 library 

Cassia X10 library is developed to support parallel programs using distributed objects over many numbers of computing nodes.
The main components of this library are `Distributed Collections`.


## Distributed Collections

This library offers the several types of distributed collections.

They offers the following facilities:

* Relocation of object elements using collective communications
  * Users can call `moveAtSync` to request object relocation that will be conducted at the next collective relocation.
  * Object elements are packed with serializer and relocated using collective communications.
* Data/element sharing between multiple collections.
  * `MoveManager` allows relocation of multiple collections together using same serializers for respective node pairs.
* The `forEach(function)` is used to apply the given `function` to all elements on respective computing nodes.
  * The calculation will be conducted using multiple worker threads.
* Dynamic insertion/deletion of elements based on key-value structures

Collection Types:

* DistCol<V> is used to hold multiple arrays placed on respective computing nodes.
  * Users can insert a kind of arrays (called chunks) each can use an arbitrary range of (long) integer indexes.
  * Users can split chunks into multiple chunks or views, and relocate them to different nodes.
  * Computing nodes can share/update index information of arrays when they call `updateDist()` collectively.
  
* DistMap<K,V> 
  * An instance of DistMap hold a HashMap instance on each computing node.
  * It allows collective relocation of elements. User can specify distribution (rule) of keys to be satisfied by the relocation.
  * DistMap does not exchange key distribution information among computing nodes.

* DistIdMap<V>
  * This is a subclass of `DistMap<Long,V>`. It limits `Long` values as keys.
  * Computing nodes can share/update key information (how keys are distributed over nodes) when they call `updateDist()` collectively.

* DistMapList<K,V>
  * This is a subclass of `DistMap<K,List<V>>`. The value is a list of `V` values.
  * In their relocation, the lists of values associated with a key is gathered from all the nodes and merged into one list.

## Cached Array

The library also support another type of collections.

* CachableArray<V> allocates cache proxies of an array on each computing node and update them iteratively.
  * It offers `broadcast(pack, unpack)` method, where `pack` and `unpack` functions are used to send/receive the modified part of object elements.
  
## Packages

* `cassia` contains our library.
* `samples` contains sample/test codes.

## Execution Environments

The cassia library assumes native version of X10 with MPI runtimes and is only tested with our customized version of X10 below.

* The customized version of X10 can be taken from [here](https://gittk.cs.kobe-u.ac.jp/kamada/x10kobecustom).
* We recommend to use thread-safe MPI.




## Publications

* Daisuke Fujishima, Tomio Kamada, "Collective Relocation for Associative Distributed Collections of Objects", International Journal of Software Innovation (IJSI)5(2), pages 15, 2017. [10.4018/IJSI.2017040104](http://doi.org/10.4018/IJSI.2017040104)
* Takuma Torii, Tomio Kamada, Kiyoshi Izumi, Kenta Yamada, "Platform design for large-scale artificial market simulation and preliminary evaluation on the K computer", Artificial Life and Robotics, Volume 22, Issue 3, pp 301–307, September 2017 [10.1007/s10015-017-0368-z](http://doi.org/10.1007/s10015-017-0368-z)
* Daisuke Fujishima, Tomio Kamada, Takumi Torii, Kiyoshi Izumi: "Overlapping Communication and Computation for Large-Scale Artificial Market Simulation", Proc. of 22nd International Symposium on Artificial Life and Robotics (AROB 2017), pp. 708-713, Beppu, Japan, Jan. 2017.

## License

Currently under [Eclipse Public License 1.0 (read here)](http://choosealicense.com/licenses/epl-1.0/).

## History

* v0.1: April 2019

## Contributors

* Tomio Kamada
* Kento Yamashita
* Daisuke Fujishima
* Takuma Torii
* Teppei Mishima
* Tosiyuki Takahashi

