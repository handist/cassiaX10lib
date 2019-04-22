package samples.rangedList;
import x10.util.*;
import x10.io.*;
import cassia.util.*;

public class TestAny {

    protected val messages: ArrayList[String];
    protected val className: String;
    private val any: Any;

    public def this(messages: ArrayList[String], any: Any) {
    	this.messages = messages;
        this.className = any.typeName();
        this.any = any;
    }

    public def testEquals(that: Any) {
        messages.add("CALL: " + className + "::equals (" + that + ")");
	try {
            messages.add("BEFORE: Any " + any.toString());
  	    val rc = any.equals(that);
	    messages.add("AFTER: Any " + any.toString());
	    messages.add("RETURN: " + rc.toString());
	} catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
	}
    }

    public def testHashCode() {
        messages.add("CALL: " + className + "::hashCode ()");
	try {
            messages.add("BEFORE: Any " + any.toString());
  	    val rc = any.hashCode();
	    messages.add("AFTER: Any " + any.toString());
	    messages.add("RETURN: " + rc.toString());
	} catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
	}
    }

    public def testToString() {
        messages.add("CALL: " + className + "::toString ()");
	try {
            messages.add("BEFORE: Any " + any.toString());
  	    val rc = any.toString();
	    messages.add("AFTER: Any " + any.toString());
	    messages.add("RETURN: " + rc);
	} catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
	}
    }

    public def testTypeName() {
        messages.add("CALL: " + className + "::typeName ()");
	try {
            messages.add("BEFORE: Any " + any.toString());
  	    val rc = any.typeName();
	    messages.add("AFTER: Any " + any.toString());
	    messages.add("RETURN: " + rc);
	} catch (e: Exception) {
	    messages.add("EXCEPTION: " + e.typeName());
	}
    }

    public def printAllMessages() {
        for (msg in messages) {
	    x10.io.Console.OUT.println(msg);
        }
    }

    public def printAllMessages(filename: String) {
        val file = new File(filename);
	val printer = file.printer();    
        for (msg in messages) {
	    printer.println(msg);
        }
	printer.flush();
    }

    public def validate(answerFile: String): Boolean {
        var rc: Boolean = true;
	var flagToString: Boolean = false;
	var flagHashCode: Boolean = false;
	var flagIterator: Boolean = false;
        val answers = new File(answerFile).lines();

	for (result in messages) {
	    if (!answers.hasNext()) {
	    	x10.io.Console.OUT.println("No more answers!");
	        return false;
	    }
	    val answer = answers.next();

	    if (result.startsWith("BEFORE: ") || result.startsWith("AFTER: ")) {
	        val result2 = result.split("@")(0);
		val answer2 = answer.split("@")(0);
                if (result2.compareTo(answer2) != 0N) {
   	            x10.io.Console.OUT.println("RESULT >>> " + result2);
	            x10.io.Console.OUT.println("ANSWER >>> " + answer2);
		    rc = false;
                }
		continue;
	    } else if (result.startsWith("CALL: ") && 
	               (result.split("::")(1).startsWith("equals") ||
		        result.split("::")(1).startsWith("sort") ||
		        result.split("::")(1).startsWith("addAllWhere") ||
			result.split("::")(1).startsWith("removeAllWhere") ||
			result.split("::")(1).startsWith("each")
			)) {
	        val result2 = result.split("@")(0);
		val answer2 = answer.split("@")(0);
                if (result2.compareTo(answer2) != 0N) {
   	            x10.io.Console.OUT.println("RESULT >>> " + result2);
	            x10.io.Console.OUT.println("ANSWER >>> " + answer2);
		    rc = false;
                }
		continue;
	    } else if (result.startsWith("CALL: ") && result.split("::")(1).startsWith("toString")) {
	        flagToString = true;
            } else if (result.startsWith("CALL: ") && result.split("::")(1).startsWith("hashCode")) {
	        flagHashCode = true;
            } else if (result.startsWith("CALL: ") && result.split("::")(1).startsWith("iterator")) {
	        flagIterator = true;
            } else if (result.startsWith("RETURN: ")) {
	        if (flagToString || flagIterator) {
		    val result2 = result.split("@")(0);
		    val answer2 = answer.split("@")(0);
                    if (result2.compareTo(answer2) != 0N) {
   	                x10.io.Console.OUT.println("RESULT >>> " + result2);
	                x10.io.Console.OUT.println("ANSWER >>> " + answer2);
		        rc = false;
                    }
  		    flagToString = false;
		    flagIterator = false;
		    continue;
                }
                if (flagHashCode) {
                    flagHashCode = false;
		    continue;
                }
            }

            if (result.compareTo(answer) != 0N) {
	        x10.io.Console.OUT.println("RESULT >>> " + result);
	        x10.io.Console.OUT.println("ANSWER >>> " + answer);
		rc = false;
            }
        }

 	if (answers.hasNext()) {
	    x10.io.Console.OUT.println("There are more answers!");
   	    rc = false;
	}	
        return rc;
    }

    public static def main(args: Rail[String]) {
        val msg = new ArrayList[String]();
	val test = new TestAny(msg, new Chunk[Long](10..20));

	test.testTypeName();
	test.printAllMessages();
    }
}
