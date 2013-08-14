package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bns.BnsCorpusFactory;
import com.fujitsu.ca.fic.dataloaders.hdfs.HdfsCorpusLoader;

public class BnsCorpusVectorizer implements CorpusVectorizer {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusVectorizer.class);

    @Override
    public void convertToSequenceFile(Configuration conf, int cardinality, String inputDirName, String outputDirName) throws IOException {
        SequenceFile.Writer writer = null;
        try {
            Path outputPath = new Path(outputDirName);
            HadoopUtil.delete(conf, outputPath);

            writer = SequenceFile.createWriter(FileSystem.get(conf), conf, outputPath, Text.class, VectorWritable.class);

            HdfsCorpusLoader<Vector> hdfsLoader = BnsCorpusFactory.createHdfsCorpusLoader(conf, inputDirName, cardinality);
            long index = 0L;
            for (Vector nextVectorizedDocument : hdfsLoader) {
                LOG.debug("Read " + index++ + "th vectorized document");
                String docLabel = ((NamedVector) nextVectorizedDocument).getName();
                String[] parts = docLabel.split(",");
                String docName = parts[0];
                String label = parts[1];

                writer.append(new Text(docName), new VectorWritable(new NamedVector(nextVectorizedDocument, label)));
            }
            LOG.info("Sequence file written to HDFS successfully. Docs written: " + index);
        } finally {
            if (writer != null)
                writer.close();
        }
    }
}
