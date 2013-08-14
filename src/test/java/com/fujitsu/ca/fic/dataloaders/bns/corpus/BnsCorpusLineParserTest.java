package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.util.List;

import org.apache.mahout.math.Vector;
import org.junit.Before;
import org.junit.Test;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;
import com.google.common.collect.Lists;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.hamcrest.Matchers.equalTo;

public class BnsCorpusLineParserTest {
    private static String correctLine1 = "(27677.txt,0),{(27677.txt,0,0,0.28095),(27677.txt,0,1,0.22829),(27677.txt,0,3,1.98149)},0.919287341982";
    private static String correctLineWithUnkToken = "(27677.txt,0),{(27677.txt,0,-1,0.28095),(27677.txt,0,1,0.22829),(27677.txt,0,3,1.98149)},0.919287341982";
    private final List<String> tokenIndexList = Lists.newArrayList("blue", "green", "red", "yellow", "orange");
    private BnsCorpusLineParser bnsLineParser;

    @Before
    public void setUp() {
        bnsLineParser = new BnsCorpusLineParser(tokenIndexList.size());
    }

    @Test
    public void parseCorrectlyFormatterLineDoesntThrowException() {
        bnsLineParser.parseFields(correctLine1);
    }

    @Test
    public void parseALineWithAnUnknownTokenShouldDoSomething() throws IncorrectLineFormatException {
        bnsLineParser.parseFields(correctLineWithUnkToken);
    }

    @Test
    public void parseACorrectLineReturnsVectorWithCorrectSizeAndValues() throws IncorrectLineFormatException {
        Vector vector = bnsLineParser.parseFields(correctLine1);
        assertThat(vector.size(), equalTo(tokenIndexList.size()));
        assertThat(vector.get(0), equalTo(0.28095));
        assertThat(vector.get(1), equalTo(0.22829));
        assertThat(vector.get(3), equalTo(1.98149));
    }
}
