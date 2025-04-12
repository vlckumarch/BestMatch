package com.bestmatch;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

// this code having a problem with duplicate matchs

public class ParallelReconciliation {

//	public static List<String> reconcile(List<Record> side1, List<Record> side2, double variance) {
//        NavigableMap<Double, Queue<Record>> side2Map = new TreeMap<>();
//
//        // Group side2 records by amount using ConcurrentLinkedQueue
//        for (Record s2 : side2) {
//            side2Map.computeIfAbsent(s2.amount, k -> new ConcurrentLinkedQueue<>()).offer(s2);
//        }
//
//        List<String> results = side1.parallelStream().map(s1 -> {
//            // Look for a match in the range [amount - variance, amount + variance]
//            Record bestMatch = null;
//
//            NavigableMap<Double, Queue<Record>> subMap = side2Map.subMap(s1.amount - variance, true, s1.amount + variance, true);
//
//            for (Map.Entry<Double, Queue<Record>> entry : subMap.entrySet()) {
//                Queue<Record> candidates = entry.getValue();
//                Record match = candidates.poll();
//                if (match != null) {
//                    bestMatch = match;
//                    break; // First closest match found
//                }
//            }
//
//            if (bestMatch != null) {
//                return "Side1: " + s1.id + " (" + s1.amount + ") <-> Side2: " + bestMatch.id + " (" + bestMatch.amount + ")";
//            } else {
//                return "Side1: " + s1.id + " (" + s1.amount + ") <-> No Match";
//            }
//        }).collect(Collectors.toList());
//
//        // Process unmatched Side2 records
//        List<String> unmatchedSide2 = side2Map.values().parallelStream()
//            .flatMap(Collection::stream)
//            .map(s2 -> "Side2: " + s2.id + " (" + s2.amount + ") <-> No Match")
//            .collect(Collectors.toList());
//
//        results.addAll(unmatchedSide2);
//        return results;
//    }
//
//    public static void main(String[] args) {
//        List<Record> side1 = new ArrayList<>();
//        List<Record> side2 = new ArrayList<>();
//
//        for (int i = 1; i <= 100000; i++) {
//            side1.add(new Record(i, Math.round(Math.random() * 1000 * 100.0) / 100.0));  // Random amounts with 2 decimals
//            side2.add(new Record(100000 + i, Math.round(Math.random() * 1000 * 100.0) / 100.0));
//        }
//
//        double variance = 0.01; // Accept difference of 1 cent
//        long startTime = System.currentTimeMillis();
//        List<String> matches = reconcile(side1, side2, variance);
//        long endTime = System.currentTimeMillis();
//
//        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
//        System.out.println("Total Matches: " + matches.size());
//
//        matches.stream().limit(10).forEach(System.out::println);
//    }
	public static List<String> reconcile(List<Record> side1, List<Record> side2, double variance) {
	    NavigableMap<Double, Queue<Record>> side2Map = new TreeMap<>();

	    for (Record s2 : side2) {
	        side2Map.computeIfAbsent(s2.amount, k -> new ConcurrentLinkedQueue<>()).offer(s2);
	    }

	    List<String> results = side1.parallelStream().map(s1 -> {
	        List<Record> potentialMatches = new ArrayList<>();

	        // Collect all possible matches within variance
	        for (Map.Entry<Double, Queue<Record>> entry : side2Map.entrySet()) {
	            double key = entry.getKey();
	            if (Math.abs(s1.amount - key) <= variance && !entry.getValue().isEmpty()) {
	                potentialMatches.addAll(entry.getValue());
	            }
	        }

	        // Select best match: min diff, then max ID
	        Record bestMatch = potentialMatches.stream()
	            .filter(r -> Math.abs(s1.amount - r.amount) <= variance)
	            .min(Comparator
	                .comparingDouble((Record r) -> Math.abs(s1.amount - r.amount))
	                .thenComparingInt(r -> -r.id)) // prefer higher ID
	            .orElse(null);

	        if (bestMatch != null) {
	            // Remove from queue
	            side2Map.get(bestMatch.amount).remove(bestMatch);
	            return "Side1: " + s1.id + " (" + s1.amount + ") <-> Side2: " + bestMatch.id + " (" + bestMatch.amount + ")";
	        } else {
	            return "Side1: " + s1.id + " (" + s1.amount + ") <-> No Match";
	        }
	    }).collect(Collectors.toList());

	    // Collect unmatched side2 records
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
//        for (int i = 1; i <= 100000; i++) {
//            side1.add(new Record(i, (int) (Math.random() * 1000)));  // Random amounts
//            side2.add(new Record(100000 + i, (int) (Math.random() * 1000)));  // Random amounts
//        }
        
        
        side1.add(new Record(1,3));
        side1.add(new Record(2,4));
        side1.add(new Record(3,4.5));
        side1.add(new Record(10,4.5));

        
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

