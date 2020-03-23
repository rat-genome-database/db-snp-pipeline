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
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Created by mtutaj on 12/13/2018.
 * <p>
 * TODO: parse studyIds and strains
 */
public class DbSnpEvaLoader {

    private String url;
    private DbSnpDao dao = new DbSnpDao();

    BufferedWriter createGZip(String fName) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(fName))));

    }
    public void run(String dir, String source, int mapKey, String groupLabel, String dumpFileName) throws Exception {


        BufferedWriter dump = createGZip(dumpFileName);
        dump.write("RS ID\tChr\tPosition\tsnpClass\tgenotype\tdbSnpBuild\tallele\tMafFreq\tMafSampleSize\tMafAllele\trefAllele\n");

        if( false ) {
            //fixAllFilesInDir("/data");
            processAllFilesInDir("/data", mapKey, dump);
            System.exit(0);
        }

        List<String> chrFiles = downloadAllChromosomes(dir, source, groupLabel, mapKey, dump);

        dump.close();

        // process files
        for( String chrFile: chrFiles ) {
            processFile(chrFile, mapKey, dump);
        }
    }

    void fixAllFilesInDir(String dirName) throws Exception {
        // process all files: temporary
        File allDir = new File(dirName);
        for (File f : allDir.listFiles()) {
            if (!f.isFile()) {
                continue;
            }
            String fname = f.getName();
            if (fname.contains("DbSnp149")) {
                String absFileName = f.getAbsolutePath();
                System.out.println(absFileName);

                // fix files
                BufferedReader in = Utils.openReader(absFileName);
                String outName = absFileName.replace("DbSnp", "dbSnp");
                BufferedWriter out = createGZip(outName);
                long lineCount = 0;
                String line;
                while( (line=in.readLine())!=null ) {
                    lineCount++;
                    if( lineCount==2 ) {
                        if( line.startsWith(",") ) {
                            continue;
                        }
                    }
                    out.write(line);
                    out.write("\n");
                }

                in.close();
                out.close();
            }
        }
    }

    // emergency procedure
    void processAllFilesInDir(String dirName, int mapKey, BufferedWriter dump) throws Exception {
        // process all files: temporary
        File allDir = new File(dirName);
        for (File f : allDir.listFiles()) {
            if (!f.isFile()) {
                continue;
            }
            String fname = f.getName();
            if (fname.startsWith("dbSnp149_cfamiliaris")) {
                System.out.println(f.getAbsolutePath());

                // fix files

                processFile(f.getAbsolutePath(), mapKey,  dump);
            }
        }
    }

    public List<String> downloadAllChromosomes(String dir, String source, String groupLabel, int mapKey, BufferedWriter dump) throws Exception {

        List<String> chrFiles = new ArrayList<>();

        String outFileNamePattern = dir + source + "_" +groupLabel + "_chr#CHR#.json";
        //String outFileNamePattern = dir + source + "_" +groupLabel + "_chr#CHR#.json.gz";

        FileDownloader fd = new FileDownloader();

        long totalVariantsWritten = 0l;

        // download all files from EVA, by chromosome, in chunks
        final int CHUNK_SIZE = 10000;
        java.util.Map<String,Integer> chromosomeSizes = dao.getChromosomeSizes(mapKey);
        List<String> chromosomes = new ArrayList<>(chromosomeSizes.keySet());
        Collections.shuffle(chromosomes);
        for( String chr: chromosomes ) {
            int fileNr = 0;
            int chrLen = chromosomeSizes.get(chr)+10000;
            String msg = "processing chromosome "+chr+" of size "+Utils.formatThousands(chrLen)+"\n";
            System.out.print(msg);
            dump.write(msg);

            String chrFileName = outFileNamePattern.replace("#CHR#", chr);
            chrFiles.add(chrFileName);
            File chrFile = new File(chrFileName);
            if( chrFile.exists() ) {
                msg = chrFileName+" already exists";
                System.out.println(msg);
                dump.write(msg+"\n");
                continue;
            }
            //BufferedWriter out = createGZip(chrFileName);
            BufferedWriter out = new BufferedWriter(new FileWriter(chrFileName));

            JsonFactory jf = new JsonFactory();
            JsonGenerator jsonGenerator = jf.createGenerator(out);
            jsonGenerator.useDefaultPrettyPrinter();
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("variants");
            jsonGenerator.writeStartArray();
            jsonGenerator.writeRaw("\n");


            String urlTemplate = getUrl().replace("#CHR#", chr).replace("#SPECIES#", groupLabel);

            int variantsWritten = 0;
            for( int i=1; i<chrLen; i+=CHUNK_SIZE ) {
                String url = urlTemplate.replace("#START#", Integer.toString(i)).replace("#STOP#", Integer.toString(i+CHUNK_SIZE-1));
                fd.setExternalFile(url);
                fd.setLocalFile("/tmp/z/" + (++fileNr) + ".json.gz");
                fd.setUseCompression(true);
                String localFile = fd.downloadNew();



                BufferedReader jsonRaw = Utils.openReader(localFile);
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

            jsonGenerator.writeEndArray();
            jsonGenerator.writeEndObject();
            jsonGenerator.close();

            out.close();

            // delete all temporary files
            File dirZ = new File("/tmp/z");
            for( File file: dirZ.listFiles() ) {
                if (!file.isDirectory() && file.getName().endsWith(".json.gz"))
                    file.delete();
            }
        }
        return chrFiles;
    }

    // process json file with all variants
    void processFile( String fname, int mapKey, BufferedWriter dump ) throws Exception {

        int snpsWithoutAccession = 0;

        BufferedReader br = Utils.openReader(fname);
        JsonFactory jf = new JsonFactory();
        JsonParser jp = jf.createParser(br);

        // keep track on the current depth of 'json objects':
        // new DB_SNP objects starts at 'jsonObjectDepth == 2'
        int jsonObjectDepth = 0;

        Map<String,Integer> unknownFields = new TreeMap<>();
        Map<Integer,Integer> snpLengths = new HashMap<>();
        DbSnp dbSnp = null;
        JsonToken token;
        while( (token=jp.nextToken()) != null) {
            if( token == JsonToken.START_OBJECT ) {
                jsonObjectDepth++;

                if( jsonObjectDepth==2 ) {
                    //System.out.println("start new db_snp object");
                    dbSnp = new DbSnp();
                    dbSnp.setMapKey(mapKey);
                }
            } else if( token == JsonToken.END_OBJECT ) {
                jsonObjectDepth--;

                if( jsonObjectDepth==1 ) {
                    if( !save(dbSnp, dump) ) {
                        snpsWithoutAccession++;
                    }
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
                            // add new maf allele to 'mafAllele' property
                            if( !dbSnp.getMafAllele().contains(mafAllele) ) {
                                dbSnp.setMafAllele(dbSnp.getMafAllele()+"/"+mafAllele);
                            }
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
                    // skip genotype "-1/-1"
                    if( genotype.equals("-1/-1") ) {
                        break;
                    }
                    if( dbSnp.getGenotype()!=null ) {
                        if( !genotype.equals(dbSnp.getGenotype()) ) {
                            // merge genotypes
                            if( !dbSnp.getGenotype().contains(genotype) ) {
                                dbSnp.setGenotype(dbSnp.getGenotype()+", "+genotype);
                            }
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
                    Integer count = snpLengths.get(snpLen);
                    if( count==null ) {
                        count = 1;
                    } else {
                        count++;
                    }
                    snpLengths.put(snpLen, count);
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

                default:
                    Integer count2 = unknownFields.get(fieldName);
                    if( count2 == null ) {
                        count2 = 1;
                    } else {
                        count2++;
                    }
                    unknownFields.put(fieldName, count2);
                    //System.out.println("unknown field "+fieldName);
            }

        }

        jp.close();
        br.close();

        // insert remaining snps
        save(null, dump);
        System.out.println("DAO: total rows inserted: "+rowsInserted);
        if( snpsWithoutAccession>0 ) {
            System.out.println("### WARN: skipped snps without accession: " + snpsWithoutAccession);
        }

        // dump unknown fields
        System.out.println();
        System.out.println("unknown fields: ");
        for( Map.Entry<String,Integer> entry: unknownFields.entrySet() ) {
            System.out.println("   "+entry.getKey()+" : "+entry.getValue());
        }

        // dump snp lengthd
        System.out.println();
        System.out.println("snp lengths distribution: ");
        for( Map.Entry<Integer,Integer> entry: snpLengths.entrySet() ) {
            System.out.println("   "+entry.getKey()+" : "+entry.getValue());
        }
        System.exit(-1);
    }

    boolean save(DbSnp dbSnp, BufferedWriter dumper) throws Exception {

        if( dbSnp!=null ) {
            if( dbSnp.getSnpName()==null ) {
                System.out.println("## no RS id for "+dbSnp.getSnpClass()+" at CHR"+dbSnp.getChromosome()+":"+dbSnp.getPosition());
                return false;
            }

            dbSnpList.add(dbSnp);
            String builder = dbSnp.getSnpName() + '\t' +
                    dbSnp.getChromosome() + '\t' +
                    dbSnp.getPosition() + '\t' +
                    dbSnp.getSnpClass() + '\t' +
                    dbSnp.getGenotype() + '\t' +
                    dbSnp.getSource() + '\t' +
                    dbSnp.getAllele() + '\t' +
                    dbSnp.getMafFrequency() + '\t' +
                    dbSnp.getMafSampleSize() + '\t' +
                    dbSnp.getMafAllele() + '\t' +
                    dbSnp.getRefAllele() + '\t' +
                    '\n';
            dumper.write(builder);

        }
        if( dbSnp==null || dbSnpList.size()>=100000 ) {
            rowsInserted += dbSnpList.size();
            dao.insert(dbSnpList);
            dbSnpList.clear();
            System.out.println("DAO: rows inserted "+rowsInserted);
        }
        return true;
    }
    static List<DbSnp> dbSnpList = new ArrayList<>(100000);
    static int rowsInserted = 0;


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
