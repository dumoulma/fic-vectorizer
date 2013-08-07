package com.fujitsu.ca.fic.dataloaders.bnscorpus;

import java.util.List;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsCorpusLineParser implements LineParser {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusLineParser.class);

    private static final int FIELDS_PER_FEATURE = 6;
    private static final int BNS_SCORE_INDEX = 4;
    private static final int TOKEN_INDEX = 3;

    private List<String> tokenIndexList;

    static int i = 0;

    @Override
    public Vector parseFields(String line) throws IncorrectLineFormatException {
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

}
