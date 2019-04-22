package samples.rangedList;
import x10.util.*;
import cassia.util.*;

public class TestChunkedList[T] {T haszero} extends TestAny {

    private val chunkedList: ChunkedList[T];

    public def this(messages: ArrayList[String], chunkedList: ChunkedList[T]) {
        super(messages, chunkedList);
	this.chunkedList = chunkedList;
    }

// class ChunkedList[T]

    public def testCheckBounds(index: Long) {
/*
        messages.add("CALL: " + className + "::checkBounds " + "(" + index + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    val rc = chunkedList.checkBounds(index);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
*/
    }

    public def testCheckDuplicate(range: LongRange) {
/*
        messages.add("CALL: " + className + "::checkDuplicate " + "(" + range + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    chunkedList.checkDuplicate(range);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
*/
    }

    public def testRanges() {
        messages.add("CALL: " + className + "::ranges " + "()");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    val rc = chunkedList.ranges();
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testAddChunk(c: RangedList[T]) {
        messages.add("CALL: " + className + "::addChunk " + "(" + c + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    chunkedList.addChunk(c);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach(op: (T)=>void) {
        messages.add("CALL: " + className + "::each " + "(" + op + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    chunkedList.each(op);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach[U](op: (T, Receiver[U])=>void, receiver: Receiver[U]) {
        messages.add("CALL: " + className + "::each " + "(" + op + ", " + receiver + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    chunkedList.each(op, receiver);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testSeparate(n: Long) {
        messages.add("CALL: " + className + "::separate " + "(" + n + ")");
	try {
            messages.add("BEFORE: ChunkedList " + chunkedList.toString());
	    val rc = chunkedList.separate(n);
	    messages.add("AFTER: ChunkedList " + chunkedList.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Scenario

    public def scenario(indexMin: Long, indexMax: Long, c1: Chunk[T], chunks: ArrayList[Chunk[T]], nseparate: Long) {

        for (i in indexMin..indexMax) {
	    if (i % 5 == 0) {
                testCheckBounds(i);
		testCheckDuplicate((i-4)..i);
            }
        } 

	testRanges();
	testAddChunk(c1);
	testEach((v: T) => { messages.add("EACH: " + v); });
	
	val receiver = new SimpleReceiver[T]();
	testEach((v: T, r: Receiver[T]) => {
	    r.receive(v);
	    messages.add("RECEIVE: " + v);
        }, receiver);
	messages.add("RECEIVER: " + receiver);

	for (c in chunks) {
	    messages.add("ADDCHUNK: " + c);
	    chunkedList.addChunk(c);
	}
	testSeparate(nseparate);

	val receiver0 = new SimpleReceiver[T]();
	chunkedList.each((v: T, r: Receiver[T]) => {
	    r.receive(v);
//	    messages.add("RECEIVE: " + v);
        }, receiver0);

	for (s in 1..(nseparate * 2)) {
	    val receiver2 = new SimpleReceiver[T]();
	    val separatedChunkedList = chunkedList.separate(s);
	    for (clist in separatedChunkedList) {
	        clist.each((v: T, r: Receiver[T]) => {r.receive(v);}, receiver2);
            }
	    val matching = receiver0.equals(receiver2);
	    messages.add("SEPARATE: splits to " + s + " chunks, have " + receiver2.size() + " values totally, matching " + matching);
        }

    }

//

    public static def main(args: Rail[String]) {

        val command = "TestChunkedList";
        if (args.size != 3) {
	    Console.OUT.println("Usage: ");
	    Console.OUT.println("    "  + command + " trace [filename1] [filename2]");
	    Console.OUT.println("        Write test log for the next validation.");
	    Console.OUT.println("    "  + command + " validate [filename1] [filename2]");
	    Console.OUT.println("        Check equality of test log with the previous trace.");
	} else if (args(0).equals("trace")) {
	    run_trace(args(1), args(2));
	} else if (args(0).equals("validate")) {
	    run_validate(args(1), args(2));
	}

    }


    public static def run_trace(filename1: String, filename2: String) {

        val c1 = new Chunk[Long](10..20);
	for (i in 10..20) {
	    c1(i) = i * i;
        }

        val c2 = new Chunk[Long](30..40);
	for (i in 30..40) {
	    c2(i) = i * i;
        }

        val c3 = new Chunk[Long](50..60);
	for (i in 50..60) {
	    c3(i) = i * i;
        }

        val c4 = new Chunk[Long](70..80);
	for (i in 70..80) {
	    c4(i) = i * i;
        }

        val c5 = new Chunk[Long](90..100);
	for (i in 90..100) {
	    c5(i) = i * i;
        }

	val chunks = new ArrayList[RangedList[Long]]();
	chunks.add(c1);
	chunks.add(c2);
	chunks.add(c3);
	chunks.add(c4);
	val chunkedList = new ChunkedList[Long](chunks);

	val chunks2 = new ArrayList[Chunk[Long]]();
	for (i in 0..14) {
	    val fromIdx = 110 + i * 20;
	    val toIdx = 120 + i * 20;
	    val c = new Chunk[Long](fromIdx..toIdx);
	    for (j in fromIdx..toIdx) {
	        c(j) = j * j;
            }
	    chunks2.add(c);
        }
	
	val msgChunkedList = new ArrayList[String]();
	val testChunkedList = new TestChunkedList(msgChunkedList, chunkedList);
	testChunkedList.scenario(10, 80, c5, chunks2, 5);
	testChunkedList.printAllMessages(filename1);
//	val resultChunkedList = testChunkedList.validate(filename1);
//	Console.OUT.println("### VALIDATION RESULT: ChunkedList " + resultChunkedList + " ###");

        val msgList = new ArrayList[String]();
	val testList = new TestList(msgList, chunkedList);
	testList.scenarioSmall(123, 225, -1);
	testList.printAllMessages(filename2);
//	val resultList = testList.validate(filename2);
//	Console.OUT.println("### VALIDATION RESULT: List " + resultList + " ###");

    }


    public static def run_validate(filename1: String, filename2: String) {

        val c1 = new Chunk[Long](10..20);
	for (i in 10..20) {
	    c1(i) = i * i;
        }

        val c2 = new Chunk[Long](30..40);
	for (i in 30..40) {
	    c2(i) = i * i;
        }

        val c3 = new Chunk[Long](50..60);
	for (i in 50..60) {
	    c3(i) = i * i;
        }

        val c4 = new Chunk[Long](70..80);
	for (i in 70..80) {
	    c4(i) = i * i;
        }

        val c5 = new Chunk[Long](90..100);
	for (i in 90..100) {
	    c5(i) = i * i;
        }

	val chunks = new ArrayList[RangedList[Long]]();
	chunks.add(c1);
	chunks.add(c2);
	chunks.add(c3);
	chunks.add(c4);
	val chunkedList = new ChunkedList[Long](chunks);

	val chunks2 = new ArrayList[Chunk[Long]]();
	for (i in 0..14) {
	    val fromIdx = 110 + i * 20;
	    val toIdx = 120 + i * 20;
	    val c = new Chunk[Long](fromIdx..toIdx);
	    for (j in fromIdx..toIdx) {
	        c(j) = j * j;
            }
	    chunks2.add(c);
        }
	
	val msgChunkedList = new ArrayList[String]();
	val testChunkedList = new TestChunkedList(msgChunkedList, chunkedList);
	testChunkedList.scenario(10, 80, c5, chunks2, 5);
//	testChunkedList.printAllMessages(filename1);
	val resultChunkedList = testChunkedList.validate(filename1);
	Console.OUT.println("### VALIDATION RESULT: ChunkedList " + resultChunkedList + " ###");

        val msgList = new ArrayList[String]();
	val testList = new TestList(msgList, chunkedList);
	testList.scenarioSmall(123, 225, -1);
//	testList.printAllMessages(filename2);
	val resultList = testList.validate(filename2);
	Console.OUT.println("### VALIDATION RESULT: List " + resultList + " ###");

    }

}
