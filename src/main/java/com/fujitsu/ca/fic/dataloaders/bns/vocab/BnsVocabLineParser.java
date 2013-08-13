package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class BnsVocabLineParser implements LineParser<Pair<String, Double>> {
    private static Logger LOG = LoggerFactory.getLogger(BnsVocabLineParser.class);

    /**
     * Parse one line of the vocab output of the bns.pig job and provide a pair<token,bns_score>.
     * <p>
     * A line is formatted: BNS_SCORE:double, TOKEN:String, COUNT:long
     * </p>
     */
    @Override
    public Pair<String, Double> parseFields(String line) throws IncorrectLineFormatException {
        LOG.debug("parseFiles: " + line);

        try {
            Double bnsScore = Double.parseDouble(line.substring(0, line.indexOf(',')));
            int tokenFieldStart = line.indexOf(',') + 1;
            int tokenFieldEnd = line.lastIndexOf(',');
            String token = line.substring(tokenFieldStart, tokenFieldEnd);
            LOG.debug(String.format("Pair: <%s, %f>", token, bnsScore));

            return new Pair<String, Double>(token, bnsScore);
        } catch (NumberFormatException nfe) {
            String message = "parseFields: could not parse BNS value in line: " + line;
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);

        } catch (ArrayIndexOutOfBoundsException aioobe) {
            String message = "parseFields: could not parse token value in line: " + line;
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);

        } catch (RuntimeException rte) {
            String message = "parseFields: Unknown error parsing line: " + line;
            LOG.warn(message);
            throw new IncorrectLineFormatException(message);
        }
    }
}
