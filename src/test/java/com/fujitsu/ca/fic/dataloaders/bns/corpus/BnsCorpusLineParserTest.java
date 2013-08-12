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
    private static String correctLine1 = "(27677.txt,0),{(27677.txt,0,blue,0.28095),(27677.txt,0,green,0.22829),(27677.txt,0,yellow,1.98149)}";
    private static String correctLineWithUnkToken = "(27677.txt,0),{(27677.txt,0,cyan,0.28095),(27677.txt,0,green,0.22829),(27677.txt,0,yellow,1.98149)}";
    private static String correctLineWithCommaInToken = "(27677.txt,0),{(27677.txt,0,100,000,0.28095),(27677.txt,0,green,0.22829),(27677.txt,0,yellow,1.98149)}";
    private final List<String> tokenIndexList = Lists.newArrayList("blue", "green", "red", "yellow", "orange");
    private final List<String> tokenIndexList2 = Lists.newArrayList("100,000", "green", "red", "yellow", "orange");

    @Before
    public void setUp() {
    }

    @Test
    public void parseCorrectlyFormatterLineDoesntThrowException() throws IncorrectLineFormatException {
        new BnsCorpusLineParser(tokenIndexList).parseFields(correctLine1);
    }

    @Test(expected = IncorrectLineFormatException.class)
    public void parseALineWithAnUnknownTokenShouldDoSomething() throws IncorrectLineFormatException {
        new BnsCorpusLineParser(tokenIndexList).parseFields(correctLineWithUnkToken);
    }

    @Test
    public void parseACorrectLineReturnsVectorWithCorrectSizeAndValues() throws IncorrectLineFormatException {
        Vector vector = new BnsCorpusLineParser(tokenIndexList).parseFields(correctLine1);
        assertThat(vector.size(), equalTo(tokenIndexList.size()));
        assertThat(vector.get(0), equalTo(0.28095));
        assertThat(vector.get(1), equalTo(0.22829));
        assertThat(vector.get(3), equalTo(1.98149));
    }

    @Test
    public void parseACorrectLineWithOneTokenWithACommaReturnsVectorWithCorrectSizeAndValues() throws IncorrectLineFormatException {
        Vector vector = new BnsCorpusLineParser(tokenIndexList2).parseFields(correctLineWithCommaInToken);
        assertThat(vector.size(), equalTo(tokenIndexList2.size()));
        assertThat(vector.get(0), equalTo(0.28095));
        assertThat(vector.get(1), equalTo(0.22829));
        assertThat(vector.get(3), equalTo(1.98149));
    }
}
