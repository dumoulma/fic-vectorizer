package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.util.List;

import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;
import com.google.common.collect.Lists;

public class BnsCorpusLineParserTest {
    private static String pigOutputFormatOK = "27677.txt,{(27677.txt,1,blue,blue,0.28095,13),(27677.txt,1,green,green,0.22829,213),(27677.txt,1,yellow,yellow,1.98149,402)}";
    List<String> tokenIndexList = Lists.newArrayList("blue", "green", "red", "yellow", "orange");

    @Test
    public void parseCorrectlyFormatterLineDoesntThrowException() throws IncorrectLineFormatException {
        LineParser<Vector> parser = new BnsCorpusLineParser(tokenIndexList);
        parser.parseFields(pigOutputFormatOK);
    }
}
