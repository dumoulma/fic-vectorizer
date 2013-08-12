package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.util.List;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsCorpusLineParser implements LineParser<Vector> {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusLineParser.class);

    private final List<String> tokenIndexList;

    public BnsCorpusLineParser(List<String> tokenIndexList) {
        this.tokenIndexList = tokenIndexList;
    }

    static int i = 0;

    @Override
    public Vector parseFields(String line) throws IncorrectLineFormatException {
        int cardinality = tokenIndexList.size();
        double[] features = new double[cardinality];
        LOG.info(i + ": " + line);

        // (27677.txt,1),{...
        String docLabelField = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
        String label = docLabelField.split(",")[1];

        // 'eat' the field,docid field and start processing fields
        line = line.substring(line.indexOf('{') + 1);

        // each field format: (27677.txt,1,rfp,0.72853)
        while (line.length() > 0) {
            int start = line.indexOf('(');
            int end = line.indexOf(')');
            String nextFeatureFields = line.substring(start + 1, end);
            try {
                addFieldToFeatures(nextFeatureFields, features);
                line = line.substring(end + 2);
            } catch (IncorrectLineFormatException e) {
                LOG.warn("parseFields: could not parse feature: " + nextFeatureFields + "\n");
                throw e;
            }
            i++;
        }
        Vector vector = new SequentialAccessSparseVector(cardinality);
        vector.assign(features);
        LOG.debug(String.format("Vector: label:%s Fields: %d", label, vector.size()));

        return new NamedVector(vector, label);
    }

    // gets DOC.txt, LABEL, TOKEN, BNS_SCORE
    // tokenized as: doc txt label token bns
    private void addFieldToFeatures(String featureField, double[] features) throws IncorrectLineFormatException {
        LOG.debug(featureField);
        int tokenFieldStart = featureField.indexOf(',', featureField.indexOf(',') + 1) + 1;
        int tokenFieldEnd = featureField.lastIndexOf(',');

        String token = featureField.substring(tokenFieldStart, tokenFieldEnd);
        int featureIndex = tokenIndexList.indexOf(token);

        // The vocabulary and the scoring are done together, the token should ALWAYS be found.
        // if it is not, the error MUST be in the pig script BNS.PIG.
        if (featureIndex == -1) {
            String message = "token --" + token + "-- was not found in the vocabulary! Check bns.pig.";
            LOG.error(message);
            throw new IncorrectLineFormatException(message);
        }
        double bnsScore = 0.0;
        String bnsScoreField = featureField.substring(tokenFieldEnd + 1);
        try {
            bnsScore = Double.parseDouble(bnsScoreField);
        } catch (NumberFormatException nfe) {
            String message = nfe.toString() + "for field: " + bnsScoreField;
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);
        }
        features[featureIndex] = bnsScore;
        LOG.debug(String.format("%s %d %f", token, featureIndex, features[featureIndex]));
    }
}
