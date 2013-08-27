package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;

public class BnsVocabLineParser implements LineParser<Pair<String, Double>> {
    private static Logger log = LoggerFactory
            .getLogger(BnsVocabLineParser.class);

    /**
     * Parse one line of the vocab output of the bns.pig job and provide a
     * pair<token,bns_score>.
     * <p>
     * A line is formatted: BNS_SCORE:double, TOKEN:String, COUNT:long
     * </p>
     */
    @Override
    public Pair<String, Double> parseFields(String line) {
        log.debug("parseFiles: " + line);
        Double bnsScore = -0.0;
        String token = "!!UNKNOWN!!";
        try {
            String[] fields = line.split(";");
            token = fields[1];
            bnsScore = Double.parseDouble(fields[0]);
            
            log.debug(String.format("Pair: <%s, %f>", token, bnsScore));

        } catch (NumberFormatException nfe) {
            String message = "parseFields: could not parse BNS value in line: "
                    + line;
            log.warn(message);

        } catch (ArrayIndexOutOfBoundsException aioobe) {
            String message = "parseFields: could not parse token value in line: "
                    + line;
            log.warn(message);

        }

        return new Pair<String, Double>(token, bnsScore);
    }
}
