package samples.rangedList;
import x10.util.*;

public class TestCollectionIterator[T] {

    private val collectionIterator: CollectionIterator[T];

    public def this(collectionIterator: CollectionIterator[T]) {
        this.collectionIterator = collectionIterator;
    }

    public def testRemove() :void {
        Console.OUT.println("### CollectionIterator[T]::testRemove ()");
	try {
	    collectionIterator.remove();
 	} catch (e: Exception) {
	    throw e;
	}
    }

}