import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class Record {
    int id;
    BigDecimal amount;
    volatile boolean matched = false;

    Record(int id, BigDecimal amount) {
        this.id = id;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Record{id=" + id + ", amount=" + amount + '}';
    }
}

class MatchedPair {
    Record side1;
    Record side2;

    MatchedPair(Record s1, Record s2) {
        this.side1 = s1;
        this.side2 = s2;
    }

    @Override
    public String toString() {
        return "MatchedPair{" +
                "side1=" + side1 +
                ", side2=" + side2 +
                '}';
    }
}

public class DecimalSafeReconciliation {

    static final BigDecimal VARIANCE = new BigDecimal("3.00");

    public static void main(String[] args) {
        List<Record> side1 = new ArrayList<>();
        List<Record> side2 = new CopyOnWriteArrayList<>();

        // Example with decimals
        side1.add(new Record(10, new BigDecimal("2.50")));
        side1.add(new Record(11, new BigDecimal("4.75")));
        side1.add(new Record(12, new BigDecimal("6.20")));

        side2.add(new Record(20, new BigDecimal("-1.60")));
        side2.add(new Record(21, new BigDecimal("-1.75")));
        side2.add(new Record(22, new BigDecimal("-3.40")));

        List<MatchedPair> matchedPairs = Collections.synchronizedList(new ArrayList<>());

        side1.parallelStream().forEach(s1 -> {
            for (Record s2 : side2) {
                if (!s2.matched) {
                    BigDecimal total = s1.amount.add(s2.amount);
                    if (total.abs().compareTo(VARIANCE) <= 0) {
                        synchronized (s2) {
                            if (!s2.matched) {
                                s2.matched = true;
                                s1.matched = true;
                                matchedPairs.add(new MatchedPair(s1, s2));
                                break;
                            }
                        }
                    }
                }
            }
        });

        System.out.println("Matched Records:");
        matchedPairs.forEach(System.out::println);

        System.out.println("\nUnmatched Side1 Records:");
        side1.stream().filter(r -> !r.matched).forEach(System.out::println);

        System.out.println("\nUnmatched Side2 Records:");
        side2.stream().filter(r -> !r.matched).forEach(System.out::println);
    }
}