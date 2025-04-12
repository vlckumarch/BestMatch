package com.bestmatch;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.bestmatch.Record;

public class BestMatch0853 {
	 public static List<String> reconcile(List<Record> side1, List<Record> side2, double variance) {
	        List<Record> side2Pool = new ArrayList<>(side2);
	        Set<Integer> matchedSide2Ids = new HashSet<>();

	        List<String> results = new ArrayList<>();

	        for (Record s1 : side1) {
	            Record bestMatch = side2Pool.stream()
	                .filter(s2 -> !matchedSide2Ids.contains(s2.id))
	                .filter(s2 -> Math.abs(s1.amount - s2.amount) <= variance)
	                .min(Comparator
	                    .comparingDouble((Record s2) -> Math.abs(s1.amount - s2.amount))
	                    .thenComparingInt(s2 -> -s2.id)) // prefer higher ID
	                .orElse(null);

	            if (bestMatch != null) {
	                matchedSide2Ids.add(bestMatch.id);
	                results.add("Side1: " + s1.id + " (" + s1.amount + ") <-> Side2: " + bestMatch.id + " (" + bestMatch.amount + ")");
	            } else {
	                results.add("Side1: " + s1.id + " (" + s1.amount + ") <-> No Match");
	            }
	        }

	        // Remaining unmatched side2
	        for (Record s2 : side2Pool) {
	            if (!matchedSide2Ids.contains(s2.id)) {
	                results.add("Side2: " + s2.id + " (" + s2.amount + ") <-> No Match");
	            }
	        }

	        return results;
	    }
	   public static void main(String[] args) {
	        List<Record> side1 = new ArrayList<>();
	        List<Record> side2 = new ArrayList<>();

	        // Generate 100K records for testing
//	        for (int i = 1; i <= 100000; i++) {
//	            side1.add(new Record(i, (int) (Math.random() * 1000)));  // Random amounts
//	            side2.add(new Record(100000 + i, (int) (Math.random() * 1000)));  // Random amounts
//	        }
	        
	        
	        side1.add(new Record(1,3));
	        side1.add(new Record(2,4));
	        side1.add(new Record(3,4.5));
	        side1.add(new Record(10,4.5));
	        side1.add(new Record(11,5.5));

	        
	        side2.add(new Record(21,3));
	        side2.add(new Record(22,4));
	        side2.add(new Record(23,5.5));
	        side2.add(new Record(24,4.5));
	        

	        double variance = 1.5;
	        long startTime = System.currentTimeMillis();
	        List<String> matches = reconcile(side1, side2, variance);
	        long endTime = System.currentTimeMillis();

	        // Print execution time
	        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
	        System.out.println("Total Matches: " + matches.size());

	        // Print first 10 results
	        matches.stream().limit(10).forEach(System.out::println);
	    }
}
