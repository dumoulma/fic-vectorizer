package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;

public class BnsCorpusLineParserTest {
    private static final int CARDINALITY = 5;
    private static String correctLine1 = "(27677.txt,0),(" + CARDINALITY
            + ",{(0,0.28095),(1,0.22829),(3,1.98149)}),0.919287341982";
    private static String correctLineWithUnkToken = "(27677.txt,0),("
            + CARDINALITY
            + ",{(-1,0.28095),(1,0.22829),(3,1.98149)}),0.919287341982";
    // private final List<String> tokenIndexList = Lists.newArrayList("blue",
    // "green", "red", "yellow", "orange");
    private final BnsCorpusLineParser bnsLineParser = new BnsCorpusLineParser();

    @Before
    public void setUp() {
    }

    @Ignore
    public void parseCorrectlyFormatterLineDoesntThrowException() {
        bnsLineParser.parseFields(correctLine1);
    }

    @Test
    public void parseALineWithAnUnknownTokenShouldIgnoreField()
            throws IncorrectLineFormatException {
        Vector vector = bnsLineParser.parseFields(correctLineWithUnkToken);
        assertThat(vector.size(), equalTo(CARDINALITY));
        assertThat(vector.get(1), equalTo(0.22829));
        assertThat(vector.get(3), equalTo(1.98149));
    }

    @Ignore
    public void parseACorrectLineReturnsVectorWithCorrectSizeAndValues()
            throws IncorrectLineFormatException {
        Vector vector = bnsLineParser.parseFields(correctLine1);
        assertThat(vector.size(), equalTo(CARDINALITY));
        assertThat(vector.get(0), equalTo(0.28095));
        assertThat(vector.get(1), equalTo(0.22829));
        assertThat(vector.get(3), equalTo(1.98149));
    }
}
