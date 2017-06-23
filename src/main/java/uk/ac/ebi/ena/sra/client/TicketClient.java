package uk.ac.ebi.ena.sra.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import htsjdk.samtools.*;
import htsjdk.samtools.cram.io.CountingInputStream;
import htsjdk.samtools.cram.ref.CRAMReferenceSource;
import htsjdk.samtools.cram.ref.ReferenceSource;
import htsjdk.samtools.seekablestream.SeekableHTTPStream;
import htsjdk.samtools.util.Log;
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
    private static enum Format {
        BAM,
        CRAM
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

        Log.setGlobalLogLevel(Log.LogLevel.ERROR);
        if (endpointUrl != null) {
            if (params.referenceName == null) {
                error("Reference name required");
            }

            Query query = new Query();
            query.sequence = params.referenceName;
            query.start = params.start;
            query.end = params.stop;
            String sURL = formatURL(endpointUrl, params.datasetId, query, params.format);
            URL url = new URL(sURL);
            TicketResponse r = getTicket(url, params.printTicket);

            IOException exception = null;
            do {
                InputStream inputStream = join(r, params.bufferSize);
                OutputStream outputStream;
                if (params.outputFile == null) outputStream = new BufferedOutputStream(System.out);
                else outputStream = new FileOutputStream(params.outputFile);
                try {
                    final byte[] buffer = new byte[params.bufferSize];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) > 0) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                } catch (IOException e) {
                    exception = e;
                    params.retries--;
                }
                outputStream.close();
            } while (params.retries > 0 && exception != null);

            return;
        }

        InputStream input = new FileInputStream(params.configurationFile);
        Yaml yaml = new Yaml(new Constructor(Configuration.class));
        Configuration configuration = (Configuration) yaml.load(input);
        diagnostics(configuration, params.bufferSize, new ReferenceSource(params.refFile));
    }

    private static void diagnostics(Configuration configuration, final int bufferSize, final CRAMReferenceSource referenceSource) {
        for (String id : configuration.test_queries.keySet()) {
            System.out.println(id);
            for (Query query : configuration.test_queries.get(id)) {
                System.out.println("\t" + query.toQueryString());
                for (String name : configuration.providers.keySet()) {
                    Provider provider = configuration.providers.get(name);
                    diagnostics(id, query, name, provider, bufferSize, referenceSource);
                }
            }
        }
    }

    private static void diagnostics(String id, Query query, String name, Provider provider, final int bufferSize, final CRAMReferenceSource referenceSource) {
        String accession = provider.accessions.get(id);
        String sURL = null;
        try {
            System.out.printf("\t\t%-10s", name);
            sURL = formatURL(provider.base, accession, query, id.contains("BAM") ? Format.BAM : Format.CRAM);
            URL url = new URL(sURL);
            TicketResponse r = getTicket(url, false);
            Report report = testTicketForQuery(r, query, bufferSize, referenceSource);
            System.out.printf("\t%20s\n", report.print());
        } catch (EndpointException e) {
            System.err.println("\tHTTP CODE " + e.code);
            System.err.println(sURL);
        } catch (Exception e) {
            System.out.println("\tUNKNOWN ERROR: " + e.getMessage());
            e.printStackTrace();
            System.err.println(sURL);
        }
    }

    private static String formatURL(String base, String accession, Query query, Format format) {
        String url = String.format("%s%s?format=%s&referenceName=%s", base, accession, format, query.sequence);
        if (query.start <= 0) query.start = 1;
        url = String.format(url + "&start=%d", query.start);
        if (query.end > 0) url = String.format(url + "&end=%d", query.end);
//        System.out.println(url);
        return url;
    }

    private static Report testTicketForQuery(TicketResponse r, Query query, final int bufferSize, CRAMReferenceSource referenceSource) throws IOException, URISyntaxException, ParseException {
        long millisStart = System.currentTimeMillis();
        InputStream inputStream = join(r, bufferSize);

        CountingInputStream cis = new CountingInputStream(inputStream);
        SamReader reader = SamReaderFactory.make().referenceSource(referenceSource)
                .open(SamInputResource.of(cis));
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

    private static InputStream join(TicketResponse response, final int bufSize) throws IOException, URISyntaxException, ParseException {
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

                    InputStream is = new LimitedInputStream(new BufferedInputStream(stream, bufSize), size);
                    inputStreamList.add(is);

                } else {
                    long size = getContentLength(new URL(uo.url));
                    InputStream is = null;
                    if (size > -1L)
                        is = new LimitedInputStream(new BufferedInputStream(new URL(uo.url).openStream(), bufSize), size);
                    else
                        is = new BufferedInputStream(new URL(uo.url).openStream(), bufSize);

                    inputStreamList.add(is);

                }
            }
        }
        return new MultiInputStream(inputStreamList);
    }

    /**
     * Try to request content length from the URL via HTTP HEAD method.
     * @param url the URL to request about
     * @return number of bytes to expect from the URL or -1 if unknown.
     */
    private static long getContentLength(URL url) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.getInputStream();
            return connection.getContentLengthLong();
        } catch (IOException e) {
            return -1L;
        } finally {
            connection.disconnect();
        }
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

        @Parameter(names = {"--format"}, description = "Format : BAM or CRAM")
        Format format=Format.BAM;

        @Parameter(names = {"--output-file"}, description = "Output file to write received data, omit for STDOUT")
        File outputFile;


        @Parameter(names = {"--print-ticket"}, description = "Print json ticket before receiving data")
        boolean printTicket = false;

        @Parameter(names = {"--buffer-size"}, description = "The buffer size to be used for downloaded data")
        int bufferSize=1024*1024;

        @Parameter(names = {"--retries"}, description = "The number of tries before declaring failure")
        int retries=3;

        @Parameter(names = {"--reference-fasta-file"}, description = "Reference fasta file to be used when reading CRAM stream")
        File refFile;
    }
}
