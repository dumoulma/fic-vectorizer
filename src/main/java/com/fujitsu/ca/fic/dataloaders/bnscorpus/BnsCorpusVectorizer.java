package com.fujitsu.ca.fic.dataloaders.bnscorpus;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusIterator;
import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.hadoop.HadoopCorpusIterator;

public class BnsCorpusVectorizer implements CorpusVectorizer {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusVectorizer.class);

    @Override
    public void convertToSequenceFile(Configuration conf, List<String> tokenIndexList, String inputDirName, String outputDirName)
            throws IOException {
        SequenceFile.Writer writer = null;
        try {
            Path outputPath = new Path(outputDirName);
            HadoopUtil.delete(conf, outputPath);

            writer = SequenceFile.createWriter(FileSystem.get(conf), conf, outputPath, LongWritable.class, VectorWritable.class);

            CorpusIterator<Vector> it = new HadoopCorpusIterator(conf, inputDirName, new BnsCorpusLineParser());
            long index = 0L;
            for (Vector nextDocVector : it) {
                writer.append(new LongWritable(index++), new VectorWritable(nextDocVector));
            }
            LOG.info("Sequence file written to HDFS successfully. Docs written: " + index);
        } finally {
            if (writer != null)
                writer.close();
        }
    }
}
