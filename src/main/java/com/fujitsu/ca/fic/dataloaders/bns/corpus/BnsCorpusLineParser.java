package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsCorpusLineParser implements LineParser<Vector> {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusLineParser.class);

    static int i = 0;

    /**
     * 
     * Expected format: (DOC_NAME:String,LABEL:int),(CARDINALITY:int,{(TOKEN_INDEX:int,BNS_SCORE:double),...}),RANDOM_NUM:double
     * 
     * @return a NamedVector which has the name doc_name,label so we can identify the document as well as know its label for supervised
     *         training
     */
    @Override
    public Vector parseFields(String line) {
        LOG.debug(i + ": " + line);

        String docLabel = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
        line = line.substring(line.indexOf(')') + 3);
        String cardinalityField = line.substring(0, line.indexOf(','));

        int cardinality = Integer.parseInt(cardinalityField);
        double[] features = new double[cardinality];

        // 'eat' the (field,docid),(cardinality, field and start processing the entries
        line = line.substring(line.indexOf('{') + 1, line.indexOf('}'));

        // each field format: (27677.txt,1,rfp,0.72853)
        while (line.length() > 0) {
            int start = line.indexOf('(');
            int end = line.indexOf(')');
            String nextFeatureFields = line.substring(start + 1, end);
            try {
                addFieldToFeatures(nextFeatureFields, features);
            } catch (IncorrectLineFormatException e) {
                LOG.warn("parseFields: could not parse feature: " + nextFeatureFields + "\nError parsing line: " + line);
            }
            line = line.substring(end + 1);
            i++;
        }
        Vector vector = new SequentialAccessSparseVector(cardinality);
        vector.assign(features);
        LOG.debug(String.format("Vector: label:%s Fields: %d", docLabel, vector.size()));

        return new NamedVector(vector, docLabel);
    }

    // gets DOC.txt, LABEL, TOKEN_INDEX, BNS_SCORE
    // tokenized as: doc txt label token_index bns
    // The vocabulary and the scoring are done together, the token should ALWAYS be found.
    // if it is not, the error MUST be in the pig script BNS.PIG.
    private void addFieldToFeatures(String featureField, double[] features) throws IncorrectLineFormatException {
        LOG.debug(featureField);
        String[] fields = featureField.split(",");
        try {
            int featureIndex = Integer.parseInt(fields[0]);

            if (featureIndex == -1) {
                String message = "unknown token index. This token is not in the vocabulary! Check bns.pig for errors";
                LOG.error(message);
                throw new IncorrectLineFormatException(message);
            }
            String bnsScoreField = fields[1];
            double bnsScore = Double.parseDouble(bnsScoreField);
            features[featureIndex] = bnsScore;

            LOG.debug(String.format("Added feature (%d,%f) to vector.", featureIndex, bnsScore));
        } catch (NumberFormatException nfe) {
            String message = nfe.toString();
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);
        }
    }
}
