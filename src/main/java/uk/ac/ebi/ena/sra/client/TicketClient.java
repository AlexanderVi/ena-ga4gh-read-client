package uk.ac.ebi.ena.sra.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import htsjdk.samtools.*;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.seekablestream.SeekableHTTPStream;
import htsjdk.samtools.util.IOUtil;
import htsjdk.samtools.util.Tuple;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.LinkedList;

/**
 * Created by vadim on 08/07/2016.
 */
public class TicketClient {
    private static void error(String message) {
        System.err.println(message);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException, URISyntaxException, EndpointException, ParseException {
        Params params = new Params();
        JCommander jc = new JCommander(params);
        jc.parse(args);

        String endpointUrl = null;
        if (params.endpointUrl != null) {
            endpointUrl = params.endpointUrl;
        } else {
            Provider provider = null;
            if (params.configurationFile != null) {
                InputStream input = new FileInputStream(params.configurationFile);
                Yaml yaml = new Yaml(new Constructor(Configuration.class));
                Configuration configuration = (Configuration) yaml.load(input);
                provider = configuration.providers.get(params.endpointName);
            }

            if (params.endpointName != null) {
                endpointUrl = provider.base;
            }
        }

        if (endpointUrl != null) {
            if (params.referenceName == null) {
                error("Reference name required");
            }
            if (params.start < 1) {
                error("Alignment start is required");
            }
            if (params.stop < 1) {
                error("Alignment end is required");
            }

            Query query = new Query();
            query.sequence = params.referenceName;
            query.start = params.start;
            query.end = params.stop;
            String sURL = formatURL(endpointUrl, params.datasetId, query);
            URL url = new URL(sURL);
            TicketResponse r = getTicket(url, params.printTicket);
            InputStream inputStream = join(r);
            OutputStream outputStream;
            if (params.outputFile == null) outputStream = new BufferedOutputStream(System.out);
            else outputStream = new FileOutputStream(params.outputFile);
            IOUtil.copyStream(inputStream, outputStream);
            outputStream.close();
            return;
        }

        InputStream input = new FileInputStream(params.configurationFile);
        Yaml yaml = new Yaml(new Constructor(Configuration.class));
        Configuration configuration = (Configuration) yaml.load(input);
        diagnostics(configuration);
    }

    private static void diagnostics(Configuration configuration) {
        for (String id : configuration.test_queries.keySet()) {
            System.out.println(id);
            for (Query query : configuration.test_queries.get(id)) {
                System.out.println("\t" + query.toQueryString());
                for (String name : configuration.providers.keySet()) {
                    Provider provider = configuration.providers.get(name);
                    diagnostics(id, query, name, provider);
                }
            }
        }
    }

    private static void diagnostics(String id, Query query, String name, Provider provider) {
        String accession = provider.accessions.get(id);
        String sURL = null;
        try {
            System.out.printf("\t\t%-10s", name);
            sURL = formatURL(provider.base, accession, query);
            URL url = new URL(sURL);
            TicketResponse r = getTicket(url, false);
            Report report = testTicketForQuery(r, query);
            System.out.printf("\t%20s\n", report.print());
        } catch (EndpointException e) {
            System.out.println("\tHTTP CODE " + e.code);
        } catch (Exception e) {
            System.out.println("\tUNKNOWN ERROR: " + e.getMessage());
            e.printStackTrace();
            System.err.println(sURL);
        }
    }

    private static String formatURL(String base, String accession, Query query) {
        return String.format("%s%s?format=BAM&referenceName=%s&start=%d&end=%d", base, accession, query.sequence, query.start, query.end);
    }

    private static Report testTicketForQuery(TicketResponse r, Query query) throws IOException, URISyntaxException, ParseException {
        long millisStart = System.currentTimeMillis();
        InputStream inputStream = join(r);

        CountingInputStream cis = new CountingInputStream(inputStream);
        SamReader reader = SamReaderFactory.make().open(SamInputResource.of(cis));
        final SAMRecordIterator iterator = reader.iterator();

        Report report = new Report();
        report.minStart = Integer.MAX_VALUE;
        report.maxEnd = -1;
        while (iterator.hasNext()) {
            final SAMRecord record;

            try {
                record = iterator.next();
            } catch (Exception e) {
                System.out.println("exception at record: " + report.reads);
                throw new RuntimeException(e);
            }
            report.reads++;

            if (!query.sequence.equals(record.getReferenceName())) {
                report.missRef++;
            } else {

                if (record.getReadUnmappedFlag()) {
                    report.unmappedReads++;
                }
                if (record.getAlignmentStart() < 1) {
                    report.noStart++;
                }

                if (record.getAlignmentStart() >= 1) {
                    report.minStart = Math.min(report.minStart, record.getAlignmentStart());
                    report.maxEnd = Math.max(report.maxEnd, record.getAlignmentEnd());
                }

                if (record.getAlignmentEnd() < query.start || record.getAlignmentStart() > query.end) {
                    report.outOfRange++;
                }
            }
        }
        report.bytes = cis.getCount();
        report.millis = System.currentTimeMillis() - millisStart;
        return report;
    }

    private static TicketResponse getTicket(URL url, boolean printTicket) throws IOException, EndpointException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();

        int code = connection.getResponseCode();
        if (code != 200) throw new EndpointException(code);
        final InputStream inputStream = connection.getInputStream();

        InputStreamReader reader = new InputStreamReader(new BufferedInputStream(inputStream), "ASCII");
        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        TicketResponse ticketResponse = gson.fromJson(reader, TicketResponse.class);
        if (printTicket) System.out.println(gson.toJson(ticketResponse));
        return ticketResponse;
    }

    private static class EndpointException extends Exception {
        int code;

        public EndpointException(int code) {
            this.code = code;
        }
    }

    private static InputStream join(TicketResponse response) throws IOException, URISyntaxException, ParseException {
        LinkedList<InputStream> inputStreamList = new LinkedList<>();
        for (TicketResponse.URL_OBJECT uo : response.urls) {
            if (uo.url.startsWith("data")) {
                final byte[] bytes = TicketResponse.fromDataURI(new URI(uo.url));
                inputStreamList.add(new ByteArrayInputStream(bytes));
            } else {
                final Tuple<Long, Long> range = uo.getRange();
                if (range != null) {
                    SeekableHTTPStream stream = new SeekableHTTPStream(new URL(uo.url));
                    stream.seek(range.a);
                    long size = range.b - range.a + 1;
                    inputStreamList.add(new LimitedInputStream(new BufferedInputStream(stream), size));

                } else {
                    inputStreamList.add(new BufferedInputStream(new URL(uo.url).openStream()));
                }
            }
        }
        return new MultiInputStream(inputStreamList);
    }

    @Parameters
    static class Params {
        @Parameter(names = {"--configuration"}, description = "A Yaml document describing providers and available test datasets")
        File configurationFile = new File("configuration.yml");

        @Parameter(names = {"--endpoint-url"}, description = "An endpoint URL to be used for quering")
        String endpointUrl;

        @Parameter(names = {"--endpoint-name"}, description = "Endpoint name to be used for quering, resolved via configuration file")
        String endpointName;

        @Parameter(names = {"--dataset-id"}, description = "Dataset id to request")
        String datasetId;

        @Parameter(names = {"--reference-name"}, description = "Reference sequence name to request")
        String referenceName;

        @Parameter(names = {"--alignment-start"}, description = "Alignment start for genomic query")
        int start=0;

        @Parameter(names = {"--alignment-stop"}, description = "Alignment end for genomic query")
        int stop=0;

        @Parameter(names = {"--output-file"}, description = "Output file to write received data, omit for STDOUT")
        File outputFile;

        @Parameter(names = {"--print-ticket"}, description = "Print json ticket before receiving data")
        boolean printTicket = false;
    }
}
