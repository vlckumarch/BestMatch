package com.bestmatch2;

import com.bestmatch.Record;

public class MatchCandidate {
    public Record side1;
    public Record side2;
    public double diff;

    public MatchCandidate(Record s1, Record s2) {
        this.side1 = s1;
        this.side2 = s2;
        this.diff = Math.abs(s1.amount - s2.amount);
    }

}
