package com.bestmatch2;
import java.util.*;
import java.util.stream.Collectors;
import com.bestmatch2.*;

public class BestMatchFinal {

    public static List<String> reconcile(List<Record2> side1, List<Record2> side2, double variance) {
        List<MatchCandidate> allCandidates = new ArrayList<>();

        for (Record2 s1 : side1) {
            for (Record2 s2 : side2) {
                double diff = Math.abs(s1.amount - s2.amount);
                if (diff <= variance) {
                    allCandidates.add(new MatchCandidate(s1, s2));
                }
            }
        }

        // Sort by best match: lowest diff, then highest side2 id
        allCandidates.sort(Comparator
                .comparingDouble((MatchCandidate m) -> m.diff)
                .thenComparingInt(m -> -m.side2.id));

        Map<Integer, Record2> matchedSide1 = new HashMap<>();
        Set<Integer> usedSide2 = new HashSet<>();

        for (MatchCandidate candidate : allCandidates) {
            int s1Id = candidate.side1.id;
            int s2Id = candidate.side2.id;

            if (!matchedSide1.containsKey(s1Id) && !usedSide2.contains(s2Id)) {
                matchedSide1.put(s1Id, candidate.side2);
                usedSide2.add(s2Id);
            }
        }

        List<String> results = new ArrayList<>();

        for (Record2 s1 : side1) {
            if (matchedSide1.containsKey(s1.id)) {
                Record2 s2 = matchedSide1.get(s1.id);
                results.add("Side1: " + s1.id + " (" + s1.amount + ") <-> Side2: " + s2.id + " (" + s2.amount + ")");
            } else {
                results.add("Side1: " + s1.id + " (" + s1.amount + ") <-> No Match");
            }
        }

        // Add unmatched side2
        for (Record2 s2 : side2) {
            if (!usedSide2.contains(s2.id)) {
                results.add("Side2: " + s2.id + " (" + s2.amount + ") <-> No Match");
            }
        }

        return results;
    }

    public static void main(String[] args) {
        List<Record2> side1 = new ArrayList<>();
        List<Record2> side2 = new ArrayList<>();

        // Generate 100K records for testing
      for (int i = 1; i <= 100000; i++) {
          side1.add(new Record2(i, (int) (Math.random() * 1000)));  // Random amounts
          side2.add(new Record2(100000 + i, (int) (Math.random() * 1000)));  // Random amounts
      }
              

//        side1.add(new Record2(1, 3));
//        side1.add(new Record2(2, 4));
//        side1.add(new Record2(3, 3.5));
////        side1.add(new Record2(10, 4.5));
////        side1.add(new Record2(11, 5.5));
//        side1.add(new Record2(12, 5.5));
//
////        side2.add(new Record2(31, 3));
//        side2.add(new Record2(32, 4));
//        side2.add(new Record2(23, 3.5));
//        side2.add(new Record2(24, 4.5));
//        side2.add(new Record2(25, 4.5));
//        side2.add(new Record2(27, 2));

        double variance = 1.5;

        long startTime = System.currentTimeMillis();
        List<String> matches = reconcile(side1, side2, variance);
        long endTime = System.currentTimeMillis();

        System.out.println("Execution Time: " + (endTime - startTime) + " ms");
        System.out.println("Total Matches: " + matches.stream().filter(s -> !s.contains("No Match")).count());

        //matches.forEach(System.out::println);
        matches.stream().limit(10).forEach(System.out::println);
        
    }
}

