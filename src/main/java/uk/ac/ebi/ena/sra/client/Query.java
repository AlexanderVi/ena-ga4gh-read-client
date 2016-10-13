package uk.ac.ebi.ena.sra.client;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vadim on 27/09/2016.
 */
class Query {
    public String sequence;
    public long start;
    public long end;

    String toQueryString() {
        return String.format("%s:%d-%d", sequence, start, end);
    }

    static Query fromString(String s) throws ParseException {
        Pattern pattern = Pattern.compile("^([^:]+):(\\d+)-(\\d+)$]");
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches() && matcher.groupCount() == 3) {
            Query query = new Query();
            query.sequence = matcher.group(1);
            query.start = Integer.valueOf(matcher.group(2));
            query.end = Integer.valueOf(matcher.group(3));
            return query;
        }
        throw new ParseException(s, 0);
    }
}
