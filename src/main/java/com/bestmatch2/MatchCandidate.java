package com.bestmatch2;



public class MatchCandidate {
    Record2 side1;
    Record2 side2;
    double diff;

    MatchCandidate(Record2 s1, Record2 s2) {
        this.side1 = s1;
        this.side2 = s2;
        this.diff = Math.abs(s1.amount - s2.amount);
    }

}
