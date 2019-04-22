package cassia.util.random;
import x10.util.Random;
import x10.lang.Math;

public class Gaussian2 {
    
    var state:Boolean;
    var g:Double;
    var random:Random;
    
    public def this(random:Random) {
        this.state = false;
        this.g = -1;
        this.random = random;
    }
    
    public def nextGaussian():Double {
        if (state) {
            state = false;
            return this.g;
        } else {
            state = true;
            var v1:Double;
            var v2:Double;
            var s:Double;
            do {
                v1 = 2.0 * random.nextDouble() - 1.0;
                v2 = 2.0 * random.nextDouble() - 1.0;
                s = v1 * v1 + v2 * v2;
            } while (s >= 1.0);
            
            var norm:Double = Math.sqrt(-2.0 * Math.log(s) / s);
            this.g = v2 * norm;
            return v1 * norm;
        }
    }
    
    public def nextGaussian(mu:Double, sigma:Double) {
        return mu + sigma * this.nextGaussian();
    }
    
    public static def main(Rail[String]) {
        val std = 0.01;
        val g = new Gaussian(new Random()); // MEMO: main()
        for (val t in 1..1000) {
            Console.OUT.println(g.nextGaussian() * std);
        }
    }

    public static def gaussDenseValue(x:Double, mu:Double, sigma:Double):Double{
        val out = Math.exp( -1*(x-mu)*(x-mu)/(2*sigma*sigma) )/Math.pow(2*Math.PI*sigma*sigma,0.5);
        return out;
    }

    public static def gaussCumulativeValue(a:Double, b:Double, delta:Double, mu:Double, sigma:Double):Double{
        assert a<b && delta > 0.0 && delta < 1.0 && !a.isInfinite() && !b.isInfinite(): "gaussCumulativeValueError";
        var out:Double = 0.0;
        val n = (b-a)/delta;
        for (var i:Long = 0; i<=n; i++) {
            val v = a + i*delta;
            out = out + gaussDenseValue(v, mu, sigma)*delta;
        }
        return out;
    }

    public static def gaussExpectedValue(a:Double, b:Double, delta:Double, mu:Double, sigma:Double):Double{
        assert a<b && delta > 0.0 && delta < 1.0 && !a.isInfinite() && !b.isInfinite(): "gaussExpectedValueError";
        var out:Double = 0.0;
        val n = (b-a)/delta;
        for (var i:Long = 0; i<=n; i++) {
            val v = a + i*delta;
            out = out + v*gaussDenseValue(v, mu, sigma)*delta;
        }
        return out;
    }


    public static def confidence(confInterval:Double):Double{
        var value:Double = 0.5;
        val delta = Math.pow(10,-5);
        var i:Long = 0;
        var x:Double = 0.00;
        do{
            x = i*delta;
            value = value + gaussDenseValue(x, 0.0, 1.0)*delta;
            i++; 
        }while(value <= confInterval);
        return x;
    }

}
