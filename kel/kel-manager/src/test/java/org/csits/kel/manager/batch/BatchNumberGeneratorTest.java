package org.csits.kel.manager.batch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BatchNumberGeneratorTest {

    @Test
    void nextBatchNumber_formatIsDateUnderscoreSequence() {
        BatchNumberGenerator generator = new BatchNumberGenerator();
        String first = generator.nextBatchNumber();
        String second = generator.nextBatchNumber();

        assertThat(first).matches("\\d{14}_\\d{3}");
        assertThat(second).matches("\\d{14}_\\d{3}");
        String[] parts1 = first.split("_");
        String[] parts2 = second.split("_");
        assertThat(parts1[0]).isEqualTo(parts2[0]);
        int seq1 = Integer.parseInt(parts1[1]);
        int seq2 = Integer.parseInt(parts2[1]);
        assertThat(seq2).isEqualTo(seq1 + 1);
    }

    @Test
    void nextBatchNumber_sequenceIncrementsWithinSameSecond() {
        BatchNumberGenerator generator = new BatchNumberGenerator();
        String a = generator.nextBatchNumber();
        String b = generator.nextBatchNumber();
        String c = generator.nextBatchNumber();

        int seqA = Integer.parseInt(a.split("_")[1]);
        int seqB = Integer.parseInt(b.split("_")[1]);
        int seqC = Integer.parseInt(c.split("_")[1]);
        assertThat(seqB).isEqualTo(seqA + 1);
        assertThat(seqC).isEqualTo(seqB + 1);
    }
}
