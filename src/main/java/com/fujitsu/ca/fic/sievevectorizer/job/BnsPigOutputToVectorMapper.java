package com.fujitsu.ca.fic.sievevectorizer.job;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusLineParser;

public class BnsPigOutputToVectorMapper extends Mapper<LongWritable, Text, LongWritable, VectorWritable> {
    private static Logger LOG = LoggerFactory.getLogger(BnsPigOutputToVectorMapper.class);

    private static final LongWritable ONE = new LongWritable(1L);
    private static final int DEFAULT_CARDINALITY = 5000;
    private LineParser<Vector> parser;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        LOG.debug("Mapping line: " + line);

        Vector nextVectorizedDocument = parser.parseFields(line);
        context.write(ONE, new VectorWritable(nextVectorizedDocument));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int cardinality = context.getConfiguration().getInt("bns.cardinality", DEFAULT_CARDINALITY);
        parser = new BnsCorpusLineParser(cardinality);
    }

    BnsPigOutputToVectorMapper() {
        super();
    }

    // NOTE: For testing purposes only!!
    BnsPigOutputToVectorMapper(LineParser<Vector> mockParser) {
        this.parser = mockParser;
    }
}
