package com.fujitsu.ca.fic.dataloaders.bnscorpus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsCorpusLoader {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusLoader.class);

    private static final int FIELDS_PER_FEATURE = 6;
    private static final int BNS_SCORE_INDEX = 4;
    private static final int TOKEN_INDEX = 3;

    private final List<String> tokenIndexList;
    private static FileSystem fs;
    private static FileStatus[] fileStatus;
    private static int currentFileStatusIndex = 0;
    private static BufferedReader reader = null;
    private static String nextLine = null;

    public BnsCorpusLoader(List<String> tokenIndexList) {
        this.tokenIndexList = tokenIndexList;
    }

    public void vectorizeToSequenceFile(Configuration conf, String inputDirName, String outputDirName) throws IOException {
        fs = FileSystem.get(conf);
        SequenceFile.Writer writer = null;
        try {
            fileStatus = fs.listStatus(new Path(inputDirName), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().matches("part(.*)");
                }
            });

            if (fileStatus.length > 0) {
                FileStatus file = fileStatus[currentFileStatusIndex];
                reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            }

            Path outputPath = new Path(outputDirName);
            HadoopUtil.delete(conf, outputPath);
            writer = SequenceFile.createWriter(fs, conf, outputPath, LongWritable.class, VectorWritable.class);
            long index = 0L;
            while (hasNext()) {
                writer.append(new LongWritable(index++), new VectorWritable(getNext()));
            }
            LOG.info("Sequence file written to HDFS successfully. Docs written: " + index);
        } finally {
            if (writer != null)
                writer.close();
        }
    }

    private Vector getNext() throws IncorrectLineFormatException {
        return parseFields(nextLine);
    }

    static int i = 0;

    // 27677.txt,{(27677.txt,1,rfp,rfp,0.72853,217),(27677.txt,1,tempor,tempor,1.02355,6)
    // DOC_ID, {(DOC_ID:String,LABEL:int,TOKEN:String,TOKEN,String,BNS_SCORE:double,TOTAL_COUNT:int)...}
    private Vector parseFields(String line) throws IncorrectLineFormatException {
        int cardinality = tokenIndexList.size();
        double[] features = new double[cardinality];
        System.out.println(i + ": " + line);

        line = line.substring(line.indexOf('{') + 1);
        int labelIndex = line.indexOf(',') + 1;
        String label = line.substring(labelIndex, labelIndex + 1);

        while (line.length() > 0) {
            int start = line.indexOf('(');
            int end = line.indexOf(')');
            String nextFeatureFields = line.substring(start + 1, end);
            try {
                addFieldToFeatures(nextFeatureFields, features);
                line = line.substring(end + 2);
            } catch (IncorrectLineFormatException e) {
                LOG.warn("parseFields: could not parse feature: " + nextFeatureFields + "\n");
            }
            i++;
        }
        Vector vector = new SequentialAccessSparseVector(cardinality);
        vector.assign(features);
        LOG.debug(String.format("Vector: label:%s Fields: %d", label, vector.size()));

        return new NamedVector(vector, label);
    }

    // gets DOC.txt, LABEL,TOKEN,TOKEN,BNS_SCORE,COUNT
    // tokenized as: doc txt label token token bns count
    private void addFieldToFeatures(String substring, double[] features) throws IncorrectLineFormatException {
        String[] parts = substring.split(",");
        // hack to fix the case where a word is a number like 100,000.00
        // should be generalized, maybe through slightly smarter exceptions
        if (parts.length == 8) {
            parts[3] = parts[2] + "," + parts[3];
            parts[4] = parts[6];
        } else if (parts.length != FIELDS_PER_FEATURE) {
            String message = "addFieldToFeatures: unexpected number of fields from split of line: " + substring;
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);
        }

        String token = parts[TOKEN_INDEX];
        int featureIndex = tokenIndexList.indexOf(token);

        // The vocabulary and the scoring are done together, the token should ALWAYS be found.
        // if it is not, the error MUST be in the pig script BNS.PIG.
        if (featureIndex == -1) {
            String message = "token --" + token + "-- was not found in the vocabulary! Check bns.pig.";
            LOG.error(message);
            throw new IncorrectLineFormatException(message);
        }

        features[featureIndex] = Double.parseDouble(parts[BNS_SCORE_INDEX]);
    }

    private boolean hasNext() {
        if (reader == null)
            return false;
        try {
            nextLine = reader.readLine();
            if (!currentFileHasNextLine(nextLine) && directoryHasMoreFiles()) {
                FileStatus file = fileStatus[++currentFileStatusIndex];
                reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                hasNext();
            } else if (currentFileHasNextLine(nextLine)) {
                return true;
            }
            reader.close();

        } catch (IOException e) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return false;
    }

    private boolean directoryHasMoreFiles() {
        return currentFileStatusIndex + 1 < fileStatus.length;
    }

    private boolean currentFileHasNextLine(String nextLine2) {
        return (nextLine != null && nextLine.length() > 0);
    }
}
