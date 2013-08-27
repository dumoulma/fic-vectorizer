package com.fujitsu.ca.fic.dataloaders.bns.corpus.sequence.mapreduce;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
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
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class VectorToSequenceReducerTest {
    private static final int CARDINALITY = 4995;
    private static final String DOCUMENT_LABEL = "data/sieve/corpus6/spam/39135.txt.gz,1";
    private static final LongWritable ONE = new LongWritable(1);

    @Mock
    private Reducer<LongWritable, VectorWritable, Text, VectorWritable>.Context context;
    @Mock
    private Configuration conf;

    private ReduceDriver<LongWritable, VectorWritable, Text, VectorWritable> reduceDriver;
    private static List<VectorWritable> vectorWritables = Lists.newArrayList();

    @BeforeClass
    public static void setUpBeforeClass() {
	vectorWritables.add(new VectorWritable(createExpectedNamedVector()));
    }

    @Before
    public void setUp() {
	when(context.getConfiguration()).thenReturn(conf);
	VectorToSequenceReducer reducer = new VectorToSequenceReducer();

	reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }

    private static NamedVector createExpectedNamedVector() {
	double[] values = new double[CARDINALITY];
	values[3488] = 1.30227;
	values[4417] = 2.51266;
	values[4418] = 2.60221;
	Vector delegate = new SequentialAccessSparseVector(CARDINALITY);
	delegate.assign(values);
	return new NamedVector(delegate, DOCUMENT_LABEL);
    }

    @Test
    public void givenOneVectorshouldCallContextWriteOnce() throws IOException, InterruptedException {
	VectorToSequenceReducer reducer = new VectorToSequenceReducer();
	reducer.reduce(ONE, vectorWritables, context);

	verify(context).write(any(Text.class), any(VectorWritable.class));
    }

    @Test
    public void shouldOutputTheExpectedConfidenceScoreForEachInputVector() throws IOException {
	reduceDriver.withInputKey(ONE);
	reduceDriver.withInputValues(vectorWritables);

	List<Pair<Text, VectorWritable>> outputs = reduceDriver.run();
	assertThat(outputs.size(), is(1));
	Pair<Text, VectorWritable> output = outputs.get(0);
	String docName = output.getFirst().toString();
	NamedVector nv = (NamedVector) output.getSecond().get();
	assertThat("data/sieve/corpus6/spam/39135.txt.gz", is(docName));
	assertThat(nv.getName(), is("1"));
    }

}
