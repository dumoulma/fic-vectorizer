package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import org.apache.pig.impl.util.Pair;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;

public class BnsVocabLineParserTest {
    private static String correctLine = "5.10394;manitoba;731";
    private static String tokenOneCommaLine = "1.0;100,000;5";
    private static String tokenThreeCommasLine = "1.0;2,000,000,000;5";
    private static BnsVocabLineParser parser = new BnsVocabLineParser();

    @Test
    public void parseACorrectLineReturnsPair() {
        Pair<String, Double> pair = parser.parseFields(correctLine);
        assertThat(pair.first, equalTo("manitoba"));
        assertThat(pair.second, equalTo(5.10394));
    }

    @Test
    public void parseALineWhereTokenHasACommaShouldReturnCorrectToken() {
        Pair<String, Double> pair = parser.parseFields(tokenOneCommaLine);
        assertThat(pair.first, equalTo("100,000"));
    }

    @Test
    public void parseALineWhereTokenHas3CommasShouldReturnCorrectToken() {
        Pair<String, Double> pair = parser.parseFields(tokenThreeCommasLine);
        assertThat(pair.first, equalTo("2,000,000,000"));
    }
}
