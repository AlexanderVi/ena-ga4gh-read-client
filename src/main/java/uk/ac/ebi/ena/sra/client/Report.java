package uk.ac.ebi.ena.sra.client;

/**
 * Created by vadim on 27/09/2016.
 */
class Report {
    long unmappedReads;
    long reads;
    long noStart;
    long missRef;
    long outOfRange;
    long minStart, maxEnd;
    long bytes;
    long millis;

    String print() {
        return String.format("%d %d %d %d %d [%s, %s], %d bytes in %d ms", reads, unmappedReads, noStart, missRef, outOfRange, (minStart == Integer.MAX_VALUE ? "-" : "" + minStart), (maxEnd < 0 ? "-" : "" + maxEnd), bytes, millis);
    }
}
