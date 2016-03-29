package com.youzan.dataalgorithms.secondarysort.spark;

// STEP-0: import required Java/Spark classes.

import com.youzan.dataalgorithms.util.SparkUtil;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * SparkSecondarySort class implemets the secondary sort design pattern
 * by sorting reducer values in memory/RAM.
 * <p>
 * <p>
 * Input:
 * <p>
 * name, time, value
 * x,2,9
 * y,2,5
 * x,1,3
 * y,1,7
 * y,3,1
 * x,3,6
 * z,1,4
 * z,2,8
 * z,3,7
 * z,4,0
 * <p>
 * Output: generate a time-series looking like this:
 * <p>
 * t1 t2 t3 t4
 * x => [3, 9, 6]
 * y => [7, 5, 1]
 * z => [4, 8, 7, 0]
 * <p>
 * x => [(1,3), (2,9), (3,6)]
 * y => [(1,7), (2,5), (3,1)]
 * z => [(1,4), (2,8), (3,7), (4,0)]
 *
 * @author Mahmoud Parsian
 */
public class SparkSecondarySort {
    public static void main(String[] args) throws Exception {

        // STEP-1: read input parameters and validate them
        if (args.length < 1) {
            System.err.println("Usage: SparkSecondarySort <file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("inputPath: <file>=" + inputPath);

        // STEP-2: Connect to the Sark master by creating JavaSparkContext object
        final JavaSparkContext ctx = SparkUtil.createJavaSparkContext();

        // STEP-3: Use ctx to create JavaRDD<String>
        //  input record format: <name><,><time><,><value>
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // STEP-4: create (key, value) pairs from JavaRDD<String> where
        // key is the {name} and value is a pair of (time, value).
        // The resulting RDD will be JavaPairRDD<String, Tuple2<Integer, Integer>>.
        // convert each record into Tuple2(name, time, value)
        // PairFunction<T, K, V>	T => Tuple2(K, V) where K=String and V=Tuple2<Integer, Integer>
        //                                                                                 input   K       V
        System.out.println("===  DEBUG STEP-4 ===");
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            public Tuple2<String, Tuple2<Integer, Integer>> call(String s) {
                String[] tokens = s.split(","); // x,2,5
                System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
            }
        });

        // STEP-5: validate STEP-4, we collect all values from JavaPairRDD<> and print it.
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2 t : output) {
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._1);
        }

        // STEP-6: We group JavaPairRDD<> elements by the key ({name}).
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        // STEP-7: validate STEP-6, we collect all values from JavaPairRDD<> and print it.
        System.out.println("===  DEBUG STEP-6 ===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output2) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        //STEP-8: Sort the reducer's values and this will give us the final output.
        // OPTION-1: worked
        // mapValues[U](f: (V) ⇒ U): JavaPairRDD[K, U]
        // Pass each value in the key-value pair RDD through a map function without changing the keys;
        // this also retains the original RDD's partitioning.
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted = groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>,      // input
                Iterable<Tuple2<Integer, Integer>>       // output
                >() {
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> s) {
                List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>(iterableToList(s));
                Collections.sort(newList, SparkTupleComparator.INSTANCE);
                return newList;
            }
        });
        //*/

        // OPTION-2: worked
        // convert each record into Tuple2(name, List<Tuple2<time, value>>) into Tuple2(name, sortedList<Tuple2<time, value>>)
        // PairFunction<T, K, V>	T => Tuple2(K, V) where K=String and V=List<Tuple2<Integer, Integer>>
        //
    /*
    JavaPairRDD<String, List<Tuple2<Integer, Integer>>> sorted = groups.map(new PairFunction<Tuple2<String, List<Tuple2<Integer, Integer>>>, // input
                                                                                             String,                                         // K
                                                                                             List<Tuple2<Integer, Integer>>>() {             // V
      public Tuple2<String, List<Tuple2<Integer, Integer>>> call(Tuple2<String, List<Tuple2<Integer, Integer>>> s) {
        List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>(s._2);
        Collections.sort(newList, SparkTupleComparator.INSTANCE);
        return new Tuple2<String, List<Tuple2<Integer, Integer>>>(s._1, newList);
      }
    });
    */


        // STEP-9: validate STEP-8, we collect all values from JavaPairRDD<> and print it.
        System.out.println("===  DEBUG STEP-8 ===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2<String, Iterable<Tuple2<Integer, Integer>>> t : output3) {
            Iterable<Tuple2<Integer, Integer>> list = t._2;
            System.out.println(t._1);
            for (Tuple2<Integer, Integer> t2 : list) {
                System.out.println(t2._1 + "," + t2._2);
            }
            System.out.println("=====");
        }

        sorted.saveAsTextFile("/mp/output");

        System.exit(0);
    }

    static List<Tuple2<Integer, Integer>> iterableToList(Iterable<Tuple2<Integer, Integer>> iterable) {
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        for (Tuple2<Integer, Integer> item : iterable) {
            list.add(item);
        }
        return list;
    }
}