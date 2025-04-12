package com.bestmatchparllel;

public class MatchCandidate {
    Record side1;
    Record side2;
    double diff;

    MatchCandidate(Record s1, Record s2) {
        this.side1 = s1;
        this.side2 = s2;
        this.diff = Math.abs(s1.amount - s2.amount);
    }
}