package com.bestmatch;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import com.bestmatch.Record;

public class BestMatchDouble {
	public static List<String> reconcile(List<Record> side1, List<Record> side2, double variance) {
	    NavigableMap<Double, Queue<Record>> side2Map = new TreeMap<>();

	    for (Record s2 : side2) {
	        side2Map.computeIfAbsent(s2.amount, k -> new ConcurrentLinkedQueue<>()).offer(s2);
	    }

	    List<String> results = side1.parallelStream().map(s1 -> {
	        Record bestMatch = null;
	        double minDiff = Double.MAX_VALUE;
	        Double bestKey = null;

	        for (Map.Entry<Double, Queue<Record>> entry : side2Map.entrySet()) {
	            double key = entry.getKey();
	            if (Math.abs(s1.amount - key) <= variance && !entry.getValue().isEmpty()) {
	                double diff = Math.abs(s1.amount - key);
	                if (diff < minDiff) {
	                    minDiff = diff;
	                    bestKey = key;
	                }
	            }
	        }

	        if (bestKey != null) {
	            bestMatch = side2Map.get(bestKey).poll();
	        }

	        if (bestMatch != null) {
	            return "Side1: " + s1.id + " (" + s1.amount + ") <-> Side2: " + bestMatch.id + " (" + bestMatch.amount + ")";
	        } else {
	            return "Side1: " + s1.id + " (" + s1.amount + ") <-> No Match";
	        }
	    }).collect(Collectors.toList());

	    List<String> unmatchedSide2 = side2Map.values().parallelStream()
	        .flatMap(Collection::stream)
	        .map(s2 -> "Side2: " + s2.id + " (" + s2.amount + ") <-> No Match")
	        .collect(Collectors.toList());

	    results.addAll(unmatchedSide2);
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
	        
	        
	        side1.add(new Record(1,4));
	        side1.add(new Record(2,3));
	        side1.add(new Record(3,4.5));

	        
	        side2.add(new Record(21,4));
	        side2.add(new Record(22,3));
	        side2.add(new Record(23,5.5));
	        

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