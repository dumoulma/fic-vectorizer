package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import org.apache.pig.impl.util.Pair;
import org.junit.Before;
import org.junit.Test;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;

public class BnsVocabLineParserTest {
    private static String correctLine = "manitoba,5.10394,731";
    private static BnsVocabLineParser parser = new BnsVocabLineParser();

    @Before
    public void setUp() {
    }

    @Test
    public void parseACorrectLineDoesntThrowException() throws IncorrectLineFormatException {
        parser.parseFields(correctLine);
    }

    @Test(expected = IncorrectLineFormatException.class)
    public void parseAnIncorrectLineThrowsException() throws IncorrectLineFormatException {
        parser.parseFields("blahblah");
    }

    @Test
    public void parseACorrectLineReturnsPair() throws IncorrectLineFormatException {
        Pair<String, Double> pair = parser.parseFields(correctLine);
        assertThat(pair.first, equalTo("manitoba"));
        assertThat(pair.second, equalTo(5.10394));
    }
}
