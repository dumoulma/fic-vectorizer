package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsVocabLineParser implements LineParser<Pair<String, Double>> {
    private static Logger LOG = LoggerFactory.getLogger(BnsVocabLineParser.class);

    @Override
    public Pair<String, Double> parseFields(String line) throws IncorrectLineFormatException {
        String token;
        Double bnsScore;

        try {
            String[] fields = line.split(",");
            if (fields.length < 3 || fields.length > 4) {
                String message = "parseFields: unexpected number of fields for line: " + line;
                LOG.warn(message);
                throw new IncorrectLineFormatException(message);
            }

            if (fields.length == 4) {
                fields[0] = fields[0] + "," + fields[1];
                fields[1] = fields[2];
            }
            token = fields[0];
            bnsScore = Double.parseDouble(fields[1]);

        } catch (Exception e) {
            LOG.warn("parseFields: could not parse line: " + line);
            throw new IncorrectLineFormatException("Could not parse line: \n" + line);
        }
        LOG.debug(String.format("Pair: <%s, %f>", token, bnsScore));
        return new Pair<String, Double>(token, bnsScore);
    }

}
