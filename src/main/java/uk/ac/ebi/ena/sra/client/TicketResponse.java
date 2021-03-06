package uk.ac.ebi.ena.sra.client;

import htsjdk.samtools.util.Tuple;

import java.net.URI;
import java.text.ParseException;
import java.util.Base64;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vadim on 22/04/2016.
 */

class TicketResponse {
    public URL_OBJECT[] urls;

    public static byte[] fromDataURI(URI uri) {
        if ("data".equalsIgnoreCase(uri.getScheme())) {
            String s = uri.getSchemeSpecificPart();
            int dataOffset = s.indexOf(',') + 1;
            return Base64.getDecoder().decode(s.substring(dataOffset));
        }
        throw new IllegalArgumentException("Not a data URI: " + uri.toASCIIString());
    }

    public static class URL_OBJECT {
        public String url;
        public TreeMap<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        public URL_OBJECT(String url) {
            this.url = url;
        }

        public void setRange(long start, long end) {
            if (start == 0 && end == 0) {
                headers.remove("Range");
                return;
            }
            if (start < 0 || end <= start) {
                throw new IllegalArgumentException(String.format("bytes=%d-%d", start, end));
            }
            headers.put("Range", String.format("bytes=%d-%d", start, end));
        }

        public Tuple<Long, Long> getRange() throws ParseException {
            if (headers == null || headers.isEmpty()) return null;

            String range = null;
            // locate Range header in a case-insensitive way:
            for (String key : headers.keySet()) {
                if ("Range".equalsIgnoreCase(key)) {
                    range = headers.get(key);
                    break;
                }
            }

            if (range == null) return null;

            final Pattern pattern = Pattern.compile("bytes=(\\d+)-(\\d+)");
            final Matcher matcher = pattern.matcher(range);
            if (matcher.matches()) {
                long start = Long.valueOf(matcher.group(1));
                long end = Long.valueOf(matcher.group(2));
                return new Tuple<>(start, end);
            } else {
                throw new ParseException("Range header: " + range, 0);
            }
        }

    }
}
