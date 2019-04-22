package samples.rangedList;
import x10.util.*;
import cassia.util.*;

public class TestChunk[T] extends TestAny {

    private val chunk: Chunk[T];

    public def this(messages: ArrayList[String], chunk: Chunk[T]) {
        super(messages, chunk);
	this.chunk = chunk;
    }

// class Chunk[T]

    public def testLast() {
        messages.add("CALL: " + className + "::last " + "()");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    val rc = chunk.last();
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testToRail() {
        messages.add("CALL: " + className + "::toRail " + "()");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    val rc = chunk.toRail();
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: " + rc.toString());
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach(op: (T)=>void) {
        messages.add("CALL: " + className + "::each " + "(" + op + ")");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    chunk.each(op);
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach(range:LongRange, op: (T)=>void) {
        messages.add("CALL: " + className + "::each " + "(" + range + ", " + op + ")");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    chunk.each(range, op);
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach[U](op: (T, Receiver[U])=>void, receiver: Receiver[U]) {
        messages.add("CALL: " + className + "::each " + "(" + op + ", " + receiver + ")");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    chunk.each(op, receiver);
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

    public def testEach[U](range:LongRange, op: (T, Receiver[U])=>void, receiver: Receiver[U]) {
        messages.add("CALL: " + className + "::each " + "(" + range + ", " + op + ", " + receiver + ")");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());
	    chunk.each(op, receiver);
	    messages.add("AFTER: Chunk " + chunk.toString());
	    messages.add("RETURN: void");
        } catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
        }
    }

// Scenario

    public def scenario(v1: T, v2: T, v3: T) {T <: x10.lang.Comparable[T]} {
        messages.add("START: " + className + " scenario Chunk");
	try {
            messages.add("BEFORE: Chunk " + chunk.toString());

	    testLast();
	    testToRail();
	    testEach((v: T) => { messages.add("EACH: " + v); });

	    val receiver = new SimpleReceiver[T]();
	    testEach((v: T, r: Receiver[T]) => {
                val idx = chunk.indexOf(v);
                if (idx % 2 == 1) {
		    messages.add("EACH: " + idx + ", " + v + " RECEIVE");
                    r.receive(v);
                } else {
		    messages.add("EACH: " + idx + ", " + v + " SKIP");
                }
            }, receiver);
	    messages.add("RECEIVER: " + receiver);
	    testEach((v: T, r: Receiver[T]) => {
                val idx = chunk.indexOf(v);
                if (idx % 2 == 0) {
		    messages.add("EACH: " + idx + ", " + v + " RECEIVE");
                    r.receive(v);
                } else {
		    messages.add("EACH: " + idx + ", " + v + " SKIP");
                }
            }, receiver);
	    messages.add("RECEIVER: " + receiver);

	    messages.add("AFTER: Chunk " + chunk.toString());
            messages.add("END: " + className + " scenario Chunk");
        } catch (e: Exception) {
	    messages.add("ABORT: " + className + " scenario Chunk " + e.typeName());
        }
    }

// 

    public static def main(args: Rail[String]) {

        val command = "TestChunk";
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
	val c = new Chunk[Long](10..20);
	for (i in 10..20) {
	    c(i) = i * i;
        }

        val msgChunk = new ArrayList[String]();
	val testChunk = new TestChunk(msgChunk, c);
	testChunk.scenario(123, 225, -1);
	testChunk.printAllMessages(filename1);
//	val resultChunk = testChunk.validate(filename1);
//	Console.OUT.println("### VALIDATION RESULT: Chunk " + resultChunk + " ###");

        val msgList = new ArrayList[String]();
	val testList = new TestList(msgList, c);
	testList.scenario(123, 225, -1);
	testList.printAllMessages(filename2);
//	val resultList = testList.validate(filename2");
//	Console.OUT.println("### VALIDATION RESULT: List " + resultList + " ###");
    }

    public static def run_validate(filename1: String, filename2: String) {
	val c = new Chunk[Long](10..20);
	for (i in 10..20) {
	    c(i) = i * i;
        }

        val msgChunk = new ArrayList[String]();
	val testChunk = new TestChunk(msgChunk, c);
	testChunk.scenario(123, 225, -1);
//	testChunk.printAllMessages(filename1);
	val resultChunk = testChunk.validate(filename1);
	Console.OUT.println("### VALIDATION RESULT: Chunk " + resultChunk + " ###");

        val msgList = new ArrayList[String]();
	val testList = new TestList(msgList, c);
	testList.scenario(123, 225, -1);
//	testList.printAllMessages(filename2);
	val resultList = testList.validate(filename2);
	Console.OUT.println("### VALIDATION RESULT: List " + resultList + " ###");
    }
}