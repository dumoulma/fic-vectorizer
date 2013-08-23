package com.fujitsu.ca.fic.sievevectorizer.job;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorToSequenceReducer extends
        Reducer<LongWritable, VectorWritable, Text, VectorWritable> {
    private static Logger log = LoggerFactory
            .getLogger(BnsPigOutputToVectorMapper.class);
    private static int vectorCount;

    @Override
    protected void reduce(LongWritable id, Iterable<VectorWritable> vectors,
            Context context) throws IOException, InterruptedException {
        for (VectorWritable nextVectorWritable : vectors) {
            NamedVector nextVectorizedDocument = (NamedVector) nextVectorWritable
                    .get();
            String docLabel = nextVectorizedDocument.getName();
            String[] parts = docLabel.split(",");
            String docName = parts[0];
            String label = parts[1];

            log.debug("Writing out " + vectorCount + "th vector of doc: "
                    + docName);
            context.write(
                    new Text(docName),
                    new VectorWritable(new NamedVector(nextVectorizedDocument
                            .getDelegate(), label)));
        }
    }
}
