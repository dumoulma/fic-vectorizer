package com.fujitsu.ca.fic.sievevectorizer.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
    private static final int CARDINALITY = 4999;

    private static final String docLabel = "data/sieve/corpus6/spam/39135.txt.gz,1";
    private static final String correctLine = "(" + docLabel + ")," + "{(data/sieve/corpus6/spam/39135.txt.gz,1,3488,1.30227),"
            + "(data/sieve/corpus6/spam/39135.txt.gz,1,4417,2.51266),"
            + "(data/sieve/corpus6/spam/39135.txt.gz,1,4418,2.60221)},0.613646770332099";

    private static final LongWritable ONE = new LongWritable(1);

    @Mock
    private Mapper<LongWritable, Text, LongWritable, VectorWritable>.Context context;
    @Mock
    private LineParser<Vector> bnsCorpusLineParser;

    private MapDriver<LongWritable, Text, LongWritable, VectorWritable> mapDriver;

    private NamedVector createExpectedNamedVector() {
        double[] values = new double[CARDINALITY];
        values[3488] = 1.30227;
        values[4417] = 2.51266;
        values[4418] = 2.60221;
        Vector delegate = new SequentialAccessSparseVector(CARDINALITY);
        delegate.assign(values);
        return new NamedVector(delegate, docLabel);
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        BnsPigOutputToVectorMapper mapper = new BnsPigOutputToVectorMapper(bnsCorpusLineParser);
        mapDriver = MapDriver.newMapDriver(mapper);

        Configuration conf = new Configuration();
        conf.setInt("bns.cardinality", CARDINALITY);
        mapDriver.setConfiguration(conf);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        NamedVector expected = createExpectedNamedVector();
        when(bnsCorpusLineParser.parseFields(anyString())).thenReturn(expected);

        mapDriver.withInput(ONE, new Text(correctLine));
        mapDriver.withOutput(ONE, new VectorWritable(expected));
        mapDriver.runTest();
    }
}
