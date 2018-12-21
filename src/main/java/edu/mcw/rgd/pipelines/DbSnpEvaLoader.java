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
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by mtutaj on 12/13/2018.
 * <p>
 * TODO: parse studyIds and strains
 */
public class DbSnpEvaLoader {

    private String url;
    private DbSnpDao dao = new DbSnpDao();

    public void run(String dir, String source, int mapKey, String groupLabel, String dumpFileName) throws Exception {

        String outFileName = dir + source + "_" +groupLabel+".json";

        String fname = outFileName + ".gz";
        processFile(fname);

        BufferedWriter dump = new BufferedWriter(new FileWriter(dumpFileName));

        // json file
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

        BufferedReader br = Utils.openReader(fname);
        JsonFactory jf = new JsonFactory();
        JsonParser jp = jf.createParser(br);

        // keep track on the current depth of 'json objects':
        // new DB_SNP objects starts at 'jsonObjectDepth == 2'
        int jsonObjectDepth = 0;

        DbSnp dbSnp = null;
        JsonToken token;
        while( (token=jp.nextToken()) != null) {
            if( token == JsonToken.START_OBJECT ) {
                jsonObjectDepth++;

                if( jsonObjectDepth==2 ) {
                    System.out.println("start new db_snp object");
                    dbSnp = new DbSnp();
                    dbSnp.setMapKey(360);
                }
            } else if( token == JsonToken.END_OBJECT ) {
                jsonObjectDepth--;

                if( jsonObjectDepth==1 ) {
                    System.out.println("save db_snp data");
                    save(dbSnp);
                }
            }

            String fieldName = jp.getCurrentName();
            if( fieldName==null ) {
                continue;
            }
            switch( fieldName ) {
                case "alternate":
                case "altAllele":
                    String allele = jp.nextTextValue();
                    if( dbSnp.getAllele()!=null ) {
                        if( !allele.equals(dbSnp.getAllele()) ) {
                            throw new Exception("allele override");
                        }
                    } else {
                        dbSnp.setAllele(allele);
                    }
                    break;
                case "mafAllele":
                    String mafAllele = jp.nextTextValue();
                    if( dbSnp.getMafAllele()!=null ) {
                        if( !mafAllele.equals(dbSnp.getMafAllele()) ) {
                            throw new Exception("maf allele override");
                        }
                    } else {
                        dbSnp.setMafAllele(mafAllele);
                    }
                    break;
                case "maf":
                    jp.nextToken();
                    dbSnp.setMafFrequency(jp.getDoubleValue());
                    break;
                case "numSamples":
                    dbSnp.setMafSampleSize(jp.nextIntValue(0));
                    break;
                case "refAllele":
                case "reference":
                    String refAllele = jp.nextTextValue();
                    if( dbSnp.getRefAllele()!=null ) {
                        if( !refAllele.equals(dbSnp.getRefAllele()) ) {
                            throw new Exception("ref allele override");
                        }
                    } else {
                        dbSnp.setRefAllele(refAllele);
                    }
                    break;
                case "type":
                case "variantType":
                    String snpType = jp.nextTextValue();
                    if( dbSnp.getSnpClass()!=null ) {
                        if( !snpType.equals(dbSnp.getSnpClass()) ) {
                            throw new Exception("snp class override");
                        }
                    } else {
                        dbSnp.setSnpClass(snpType);
                    }
                    break;
                case "format":
                    String format = jp.nextTextValue();
                    if( !format.equals("GT") ) {
                        throw new Exception("unexpected format: "+format);
                    }
                    break;
                case "GT":
                    String genotype = jp.nextTextValue();
                    if( dbSnp.getGenotype()!=null ) {
                        if( !genotype.equals(dbSnp.getGenotype()) ) {
                            throw new Exception("genotype override");
                        }
                    } else {
                        dbSnp.setGenotype(genotype);
                    }
                    break;
                case "dbsnp-build":
                    dbSnp.setSource("dbSnp"+jp.nextTextValue());
                    break;
                case "chromosome":
                    dbSnp.setChromosome(jp.nextTextValue());
                    break;
                case "start":
                    dbSnp.setPosition(jp.nextIntValue(0));
                    break;
                case "length":
                    int snpLen = jp.nextIntValue(0);
                    if( snpLen!=1 ) {
                        throw new Exception("unexpected snp len: "+snpLen);
                    }
                    break;
                case "ids":
                    if( jp.nextToken() != JsonToken.START_ARRAY ) {
                        throw new Exception("unexpected: no array for ids");
                    }
                    while( jp.nextToken()!= JsonToken.END_ARRAY ) {
                        String id = jp.getText();
                        if( id.startsWith("rs") ) {
                            dbSnp.setSnpName(id);
                        }
                    }
                    break;
                case "genomic":
                    if( jp.nextToken() != JsonToken.START_ARRAY ) {
                        throw new Exception("unexpected: no array for genomic");
                    }
                    while( jp.nextToken()!= JsonToken.END_ARRAY ) {
                        String id = jp.getText();
                        if( id.contains(":g.") ) {
                            if( dbSnp.getHgvs()==null ) {
                                dbSnp.setHgvs(new ArrayList<>());
                            }
                            dbSnp.getHgvs().add(id);
                        }
                    }
                    break;

                case "altAlleleCount": // ignore this for a moment
                case "altAlleleFreq": // ignore this for a moment
                case "mgf": // ignore this for a moment
                case "missingAlleles": // ignore this for a moment
                case "missingGenotypes": // ignore this for a moment
                case "refAlleleCount": // ignore this for a moment
                case "refAlleleFreq": // ignore this for a moment
                case "transition": // ignore this for a moment
                case "transversion": // ignore this for a moment

                case "attributes":
                case "cohortStats":
                case "end":
                case "fileId":
                case "hgvs":
                case "samplesData":
                case "sourceEntries":
                case "studyId":
                case "variants": // toplevel array of all db_snp variants
                    break;

                case "2010-1_SHR_2010-1_SHR":
                case "ENSEMBL_CELERA":
                case "ENSEMBL_CELERA_maf":
                case "ENSEMBL_SHR_RAT":
                case "ENSEMBL_SHR_RAT_maf":
                case "RAT_COMPUTATIONAL_CELERA_RAT_COMPUTATIONAL_CELERA":
                case "SD":
                case "SHR/OLAIPCV":
                    break;
                default:
                    System.out.println("unknown field "+fieldName);
            }

        }

        jp.close();
        br.close();

        System.exit(-1);
    }

    void save(DbSnp dbSnp) {
        System.out.println("save db_snp\n");
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
