package edu.mcw.rgd.pipelines;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;

import java.io.*;
import java.math.BigDecimal;
import java.util.Map;

/**
 * Created by mtutaj on 12/13/2018.
 */
public class DbSnpEvaLoader {

    private String url;
    private DbSnpDao dao = new DbSnpDao();

    public void run(String dir, String source, int mapKey, String groupLabel, String dumpFileName) throws Exception {

        BufferedWriter dump = new BufferedWriter(new FileWriter(dumpFileName));

        // json file
        String outFileName = dir + source + "_" +groupLabel+".json";
        BufferedWriter out = new BufferedWriter(new FileWriter(outFileName));
        JsonFactory jf = new JsonFactory();
        JsonGenerator jsonGenerator = jf.createGenerator(out);
        jsonGenerator.useDefaultPrettyPrinter();
        jsonGenerator.writeStartObject();
        jsonGenerator.writeFieldName("variants");
        jsonGenerator.writeStartArray();
        jsonGenerator.writeRaw("\n");

        FileDownloader fd = new FileDownloader();
        fd.setDoNotUseHttpClient(true);
        fd.setMaxRetryCount(1);

        int fileNr = 0;
        long totalVariantsWritten = 0l;

        // download all files from EVA, by chromosome, in 100,000 chunks
        final int CHUNK_SIZE = 100000;
        java.util.Map<String,Integer> chromosomeSizes = dao.getChromosomeSizes(mapKey);
        for( Map.Entry<String,Integer> entry: chromosomeSizes.entrySet() ) {
            String chr = entry.getKey();
            int chrLen = entry.getValue()+100000;
            String msg = "processing chromosome "+chr+" of size "+Utils.formatThousands(chrLen)+"\n";
            System.out.print(msg);
            dump.write(msg);

            String urlTemplate = getUrl().replace("#CHR#", chr).replace("#SPECIES#", groupLabel);

            int variantsWritten = 0;
            for( int i=1; i<chrLen; i+=CHUNK_SIZE ) {
                String url = urlTemplate.replace("#START#", Integer.toString(i)).replace("#STOP#", Integer.toString(i+CHUNK_SIZE-1));
                fd.setExternalFile(url);
                fd.setLocalFile("/tmp/z/" + (++fileNr) + ".json");
                String localFile = fd.downloadNew();



                String jsonRaw = Utils.readFileAsString(localFile);
                JsonObject obj = (JsonObject) Jsoner.deserialize(jsonRaw);
                JsonArray arr = (JsonArray) obj.get("response");
                for( int j=0; j<arr.size(); j++) {
                    JsonObject response = (JsonObject) arr.get(j);
                    int numResults = ((BigDecimal) response.get("numResults")).intValueExact();
                    int numTotalResults = ((BigDecimal) response.get("numTotalResults")).intValueExact();
                    if( numResults<numTotalResults ) {
                        System.out.println("*** serious problem: numResults<numTotalResults");
                        dump.write("*** serious problem: numResults<numTotalResults\n");
                    }

                    Double progressInPercent = (100.0*i)/(chrLen);
                    String progress = ",  "+String.format("%.1f%%", progressInPercent);
                    msg = "  chr"+chr+":"+i+"-"+(i+CHUNK_SIZE-1)+"   variants:"+numResults+" chr total:"+variantsWritten+progress+"\n";
                    System.out.print(msg);
                    dump.write(msg);

                    if( numResults==0 ) {
                        continue;
                    }
                    JsonArray result = (JsonArray) response.get("result");
                    for( int k=0; k<result.size(); k++ ) {
                        JsonObject o = (JsonObject) result.get(k);
                        if( variantsWritten>0 ) {
                            jsonGenerator.writeRaw(",\n");
                        }
                        jsonGenerator.writeRaw(o.toJson());
                        variantsWritten++;

                        // finish test
                        if( variantsWritten>=Integer.MAX_VALUE ) {
                            jsonGenerator.writeEndArray();
                            jsonGenerator.writeEndObject();
                            jsonGenerator.close();
                            out.close();
                            dump.close();
                            throw new Exception("BREAK");
                        }
                    }
                }
            }
            totalVariantsWritten += variantsWritten;
            msg = "============\n";
            msg += "chr"+chr+", variants written: "+Utils.formatThousands(variantsWritten)
                    +",  total variants written: "+Utils.formatThousands(totalVariantsWritten)+"\n";
            msg += "============\n\n";
            System.out.print(msg);
            dump.write(msg);
            dump.flush();
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
        jsonGenerator.close();
        out.close();

        dump.close();
    }

    // process json file with all variants
    void processFile( String fname ) throws Exception {

        JsonFactory jf = new JsonFactory();
        JsonParser jp = jf.createParser(new BufferedReader(new FileReader(fname)));
        if (jp.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected data to start with an Object");
        }

        // reference is the first token
        while (jp.nextToken() != null) {
            String fieldname = jp.getCurrentName();
            /*
            if ("variants".equals(fieldname)) {
                jParser.nextToken();
                parsedName = jParser.getText();
            }

            if ("age".equals(fieldname)) {
                jParser.nextToken();
                parsedAge = jParser.getIntValue();
            }

            if ("address".equals(fieldname)) {
                jParser.nextToken();
                while (jParser.nextToken() != JsonToken.END_ARRAY) {
                    addresses.add(jParser.getText());
                }
            }
            */
        }

        jp.close();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
