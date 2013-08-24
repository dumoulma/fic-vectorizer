package com.fujitsu.ca.fic.dataloaders.bns.corpus.sequence.mapreduce;

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
    private static Logger log = LoggerFactory.getLogger(BnsPigOutputToVectorMapper.class);

    private static final LongWritable ONE = new LongWritable(1L);
    private final LineParser<Vector> parser;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        log.debug("Mapping line: " + line);

        Vector nextVectorizedDocument = parser.parseFields(line);
        context.write(ONE, new VectorWritable(nextVectorizedDocument));
    }

    BnsPigOutputToVectorMapper() {
        super();
        parser = new BnsCorpusLineParser();
    }

    // NOTE: For testing purposes only!!
    BnsPigOutputToVectorMapper(LineParser<Vector> mockParser) {
        this.parser = mockParser;
    }
}
