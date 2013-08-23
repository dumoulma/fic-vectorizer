package com.fujitsu.ca.fic.sievevectorizer.job;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fujitsu.ca.fic.dataloaders.LineParser;

import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BnsPigOutputToVectorMapperTest {
    private static final int CARDINALITY = 3672;
    private static final String DOC_LABEL = "data/sieve/corpus6/spam/39135.txt.gz,1";
    // (data/sieve/corpus6/spam/39252.txt.gz,1),(3672,{(480,0.38624),(1474,0.03848),(570,0.74978),(3281,1.11081)
    private static final String SAMPLE_LINE = "("
            + DOC_LABEL
            + "),("
            + CARDINALITY
            + ",{(480,0.38624),(1474,0.03848),(570,0.74978),(3281,1.11081)},0.613646770332099";

    private static final LongWritable ONE = new LongWritable(1);

    @Mock
    private Mapper<LongWritable, Text, LongWritable, VectorWritable>.Context context;
    @Mock
    private LineParser<Vector> bnsCorpusLineParser;

    private MapDriver<LongWritable, Text, LongWritable, VectorWritable> mapDriver;

    private NamedVector createExpectedNamedVector() {
        double[] values = new double[CARDINALITY];
        values[480] = 0.38624;
        values[1474] = 0.03848;
        values[570] = 0.74978;
        values[3281] = 1.11081;
        Vector delegate = new SequentialAccessSparseVector(CARDINALITY);
        delegate.assign(values);
        return new NamedVector(delegate, DOC_LABEL);
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        BnsPigOutputToVectorMapper mapper = new BnsPigOutputToVectorMapper(
                bnsCorpusLineParser);
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        NamedVector expected = createExpectedNamedVector();
        when(bnsCorpusLineParser.parseFields(anyString())).thenReturn(expected);

        mapDriver.withInput(ONE, new Text(SAMPLE_LINE));
        mapDriver.withOutput(ONE, new VectorWritable(expected));
        mapDriver.runTest();
    }
}
