package com.groupby;

import java.util.*;
import java.util.stream.Collectors;





public class GroupedBestMatch {

    public static List<String> reconcile(List<Record> side1, List<Record> side2, double variance) {
        // Group both sides by internal reference
    	Map<String, List<Record>> side1ByRef = side1.stream()
    		    .collect(Collectors.groupingBy((Record r) -> r.ref));
    	Map<String, List<Record>> side2ByRef = side2.stream()
    		    .collect(Collectors.groupingBy((Record r) -> r.ref));
    	


        List<String> allResults = new ArrayList<>();

        for (String ref : side1ByRef.keySet()) {
            List<Record> s1Group = side1ByRef.get(ref);
            List<Record> s2Group = side2ByRef.getOrDefault(ref, Collections.emptyList());

            // Step 1: Parallel scoring
            List<MatchCandidate> candidates = s1Group.parallelStream()
                .flatMap(s1 -> s2Group.stream()
                    .filter(s2 -> Math.abs(s1.amount - s2.amount) <= variance)
                    .map(s2 -> new MatchCandidate(s1, s2)))
                .collect(Collectors.toList());

            // Step 2: Sort candidates
            candidates.sort(Comparator.<MatchCandidate>comparingDouble(m -> m.diff)
                .thenComparingInt(m -> -m.side2.id));

            // Step 3: Assign matches without duplicates
            Map<Integer, Record> matchedS1 = new HashMap<>();
            Set<Integer> usedS2 = new HashSet<>();

            for (MatchCandidate c : candidates) {
                if (!matchedS1.containsKey(c.side1.id) && !usedS2.contains(c.side2.id)) {
                    matchedS1.put(c.side1.id, c.side2);
                    usedS2.add(c.side2.id);
                }
            }

            // Step 4: Format results
            for (Record s1 : s1Group) {
                if (matchedS1.containsKey(s1.id)) {
                    Record s2 = matchedS1.get(s1.id);
                    allResults.add("Side1: " + s1.id + " (" + s1.amount + ", " + s1.ref + ") <-> Side2: " + s2.id + " (" + s2.amount + ", " + s2.ref + ")");
                } else {
                    allResults.add("Side1: " + s1.id + " (" + s1.amount + ", " + s1.ref + ") <-> No Match");
                }
            }

            for (Record s2 : s2Group) {
                if (!usedS2.contains(s2.id)) {
                    allResults.add("Side2: " + s2.id + " (" + s2.amount + ", " + s2.ref + ") <-> No Match");
                }
            }
        }

        return allResults;
    }


    public static void main(String[] args) {
        List<Record> side1 = List.of(
            new Record(1, 3, "ABC123"),
            new Record(5, 3, "ABC123"),
            new Record(2, 3, "ABC124"),
            new Record(3, 4, "ABC124")
        );

        List<Record> side2 = List.of(
            new Record(11, 3, "ABC123"),
            new Record(21, 3.5, "ABC124"),
            new Record(31, 4, "ABC124"),
            new Record(41, 2.5, "ABC123")
        );

        double variance = 1.0;
        List<String> results = reconcile(side1, side2, variance);

        System.out.println("Total Matches: " + results.stream().filter(s -> s.contains("<-> Side2")).count());
        results.forEach(System.out::println);
    }
}