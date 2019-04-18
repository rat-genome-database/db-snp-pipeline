package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.FastaParser;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.process.mapping.MapManager;
import nu.xom.*;
import org.apache.commons.logging.*;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import edu.mcw.rgd.xml.*;

import java.io.*;
import java.sql.ResultSet;
import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since 9/27/11
 * load manager
 */
public class DbSnpLoader {

    protected final Log logger = LogFactory.getLog("dbsnp");
    private String version;

    DbSnpDao dao;
    FileDownloader downloader = new FileDownloader();
    FastaParser fastaParser = new FastaParser();
    int skippedDbSnpEntries = 0;
    int skippedDbSnpEntriesWithoutClinicalSignificance = 0;

    /**
     * parse cmd line arguments and run the loader;
     * @param args cmd line arguments: -data_dir {data_dir_path} -map_key {map_key_value} -group_label {GRCh37.p2} -build {dbSnp134}
     * @throws Exception
     */
    static public void main(String[] args) throws Exception {

        String dataDir = null;
        int mapKey = 0;
        String build = null;
        String groupLabel = null;
        String exportFile = null;
        String importFile = null;
        String dbSnpTableName = "DB_SNP";
        boolean upConvert = false;
        boolean noDumpFile = false;
        boolean withClinicalSignificance = false;

        // parse cmd line for args
        for( int argp=0; argp<args.length; argp++ ) {
            switch (args[argp]) {
                case "-data_dir":
                    dataDir = args[++argp];
                    break;
                case "-map_key":
                    mapKey = Integer.parseInt(args[++argp]);
                    break;
                case "-group_label":
                    groupLabel = args[++argp];
                    break;
                case "-build":
                    build = args[++argp];
                    break;
                case "-export_file":
                    exportFile = args[++argp];
                    break;
                case "-import_file":
                    importFile = args[++argp];
                    break;
                case "-up_convert":
                    upConvert = true;
                    break;
                case "-no_dump_file":
                    noDumpFile = true;
                    break;
                case "-with_clinical_significance":
                    withClinicalSignificance = true;
                    break;
                case "-table":
                    dbSnpTableName = args[++argp];
                    break;

            }
        }

        // validate cmd line params
        if( dataDir==null || mapKey==0 || build==null || groupLabel==null) {
            System.out.println("Usage: java -Dspring.config={db.conf.file} -jar DbSnpLoader.jar -data_dir {data_dir_path} -map_key {map_key_value} -group_label {group_label} -build {build} [-no_dump_file] [-table db_snp_table_name]");
            System.out.println("  for example:");
            System.out.println("  java -Dspring.config=../properties/default_db.xml -jar DbSnpLoader.jar -data_dir ./data/dbSnp134 -map_key 17 -group_label GRCh37.p2 -build dbSnp134");
            return;
        }

        // instantiate loader class
	    XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        DbSnpLoader loader=(DbSnpLoader) (bf.getBean("loader"));

        // set DB_SNP table name
        if( dbSnpTableName!=null ) {
            loader.dao.setTableName(dbSnpTableName);
            System.out.println("dbSnp table used for import is: "+dbSnpTableName);
        }

        String result;
        if( exportFile!=null ) {
            result = loader.exportData(build, mapKey, exportFile);
        }
        else if( importFile!=null ) {
            result = loader.importData(build, mapKey, importFile);
        }
        else if( upConvert ) {
            loader.dao.upConvert(build, 17, 13);
            result = "OK";
        }
        else {
            String dumpFile = noDumpFile ? null : "data/load.log";
            result = "OK";

            MapManager mm = MapManager.getInstance();
            edu.mcw.rgd.datamodel.Map map = mm.getMap(mapKey);
            if( map.getSpeciesTypeKey()!=SpeciesType.HUMAN ) {
                DbSnpEvaLoader evaLoader=(DbSnpEvaLoader) (bf.getBean("evaLoader"));
                evaLoader.run(dataDir, build, mapKey, groupLabel, dumpFile);
            } else {
                loader.run(dataDir, build, mapKey, groupLabel, dumpFile, withClinicalSignificance);
            }
        }
        System.out.println(result);
    }

    // optional dump data writer
    BufferedWriter dumpDataWriter;

    public String importData(String source, int mapKey, String inFileName) throws Exception {

        BufferedReader importFile = new BufferedReader(new FileReader(inFileName));

        String line;
        while( (line=importFile.readLine())!=null ) {
            String[] cols = line.split("\\s", -1);
            DbSnp dbSnp = new DbSnp();
            dbSnp.setChromosome(cols[0]);
            dbSnp.setPosition(Integer.parseInt(cols[1]));
            dbSnp.setSnpName(cols[2]);
            dbSnp.setMapKey(mapKey);
            dbSnp.setSource(source);
            insertToDb(dbSnp);
        }

        insertToDb(null); // signal end of stream of DbSnp objects
        return "OK";
    }

    static List<DbSnp> dbSnpList = new ArrayList<>(100000);
    static int rowsInserted = 0;
    private void insertToDb(DbSnp dbSnp) throws Exception {
        if( dbSnp!=null )
            dbSnpList.add(dbSnp);
        if( dbSnp==null || dbSnpList.size()>=100000 ) {
            rowsInserted += dbSnpList.size();
            dao.insert(dbSnpList);
            dbSnpList.clear();
            System.out.println("rows inserted "+rowsInserted);
        }
    }

    public String exportData(String source, int mapKey, String dumpFileName) throws Exception {

        //logger.info("EProcessing dir " + dir + " for source " + source + " and mapKey=" + mapKey + ", groupLabel="+groupLabel);

        BufferedWriter exportFile = new BufferedWriter(new FileWriter(dumpFileName));

        ResultSet rs = null;
        try {
            rs = dao.getDataSet(source, mapKey);
            while( rs.next() ) {
                exportFile.write(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3)+"\n");
            }

        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if( rs!=null ) {
                    rs.getStatement().getConnection().close();
                }
            } catch(Exception e) {}

            exportFile.close();
        }
        return "OK";
    }

    /**
     *
     * @param dir directory with dbsnp xml files
     * @param source name of data source, like 'dbSnp130'
     * @param mapKey map key related to the data source ('13' for human assembly 36.x)
     * @return status string
     * @throws Exception
     */
    public String run(String dir, String source, int mapKey, String groupLabel, String dumpFileName, boolean withClinicalSignificance) throws Exception {

        logger.info("Processing dir " + dir + " for source " + source + " and mapKey=" + mapKey + ", groupLabel="+groupLabel);

        long timestamp = System.currentTimeMillis();

        // iterate all subdirectories; they should be in the format: ds_ch1.xml.gz, ds_ch2.xml.gz ...
        if( dir==null || dir.equals("") ) {
            return "Data directory not given";
        }
        File rootDir = new File(dir);
        if( !rootDir.isDirectory() ) {
            return dir+" is not a directory";
        }

        // try to open the optional dump file
        if( dumpFileName!=null && dumpFileName.trim().length()>0 ) {
            dumpDataWriter = new BufferedWriter(new FileWriter(dumpFileName));
            dumpDataWriter.write("RS ID\tChr\tPosition\tHetType\tHetScore\tStdErr\tsnpClass\tsnpType\tmolType\tgenotype\tdbSnpBuild\tgenomeBuild\tallele\tstrand\tMafFreq\tMafSampleSize\tMafAllele\tMapLocCount\tFxnClass\trefAllele\n");
        }

        // randomize order of file names
        List<File> files = Arrays.asList(rootDir.listFiles());
        Collections.shuffle(files);

        for( File file: files ) {
            // process only files
            if( file.isDirectory() )
                continue;
            // the file name should be like ds_ch?.xml.gz
            String fname = file.getName();
            if( fname.startsWith("ds_ch") && fname.endsWith(".xml.gz") ) {
                // extract chromosome
                int dotPos = fname.indexOf('.');
                String chr = fname.substring(5, dotPos);
                // chromosome must be 1 or 2 characters long -- ignore the others
                if( (chr.length()==1 || chr.length()==2) && !chr.equalsIgnoreCase("Un") ) {
                    // process the data
                    long timestamp1 = System.currentTimeMillis();
                    String msg = processChromosome(file, chr, source, mapKey, groupLabel, withClinicalSignificance);
                    System.out.println(msg);
                    dao.printStats();
                    long timestamp2 = System.currentTimeMillis();
                    System.out.println("TIME ELAPSED: "+Utils.formatElapsedTime(timestamp1, timestamp2));
                    System.out.println("==================\n");
                }
                else {
                    logger.info("!!! Skipping file "+file.getAbsolutePath());
                }
            }
        }

        long minutes = (System.currentTimeMillis()-timestamp)/(60*1000);
        logger.info("DONE! "+minutes+" minutes");
        logger.info("Done processing directory "+dir+" for source "+source+ " and mapKey="+mapKey);

        if( dumpDataWriter!=null )
            dumpDataWriter.close();

        return "OK";
    }

    String processChromosome(File file, String chr, String source, int mapKey, String groupLabel, boolean withClinicalSignificance) throws Exception  {

        skippedDbSnpEntries = 0;
        skippedDbSnpEntriesWithoutClinicalSignificance = 0;

        // create new xom analyzer for every file
        MyXomAnalyzer xmlParser = new MyXomAnalyzer();
        xmlParser.setValidate(false);
        xmlParser.init(dumpDataWriter, dao, source, mapKey, groupLabel, chr, withClinicalSignificance);

        xmlParser.parse(file.getAbsoluteFile());

        xmlParser.flush();

        if( dumpDataWriter!=null )
            dumpDataWriter.flush();

        String msg = file.getName()+" was parsed successfully\n"+
               skippedDbSnpEntries+" non-snp dbSnps have been skipped from loading\n";
        if( withClinicalSignificance ) {
            msg += skippedDbSnpEntriesWithoutClinicalSignificance+" dbSnps without clinical significance info have been skipped from loading\n";
        }
        return msg;
    }

    public DbSnpDao getDao() {
        return dao;
    }

    public void setDao(DbSnpDao dao) {
        this.dao = dao;
    }

    /// slow (remote) version to get the reference nucleotide
    String getRefNucleotideFromService(String chr, int pos, int mapKey) throws Exception {

        downloader.setExternalFile("http://kyle.rgd.mcw.edu/rgdweb/seqretrieve/retrieve.html?mapKey="+mapKey+"&chr="+chr+
                "&startPos="+pos+"&stopPos="+pos+"&format=text");
        downloader.setLocalFile("/tmp/"+chr+"."+pos);
        String localFile = downloader.download();
        String refNuc = Utils.readFileAsString(localFile);
        new File(localFile).delete();
        return refNuc;
    }

    /// fast (local) version to get the reference nucleotide
    String getRefNucleotide(String chr, int pos, int mapKey) throws Exception {

        fastaParser.setMapKey(mapKey);
        return fastaParser.getSequence(chr, pos, pos);
    }

    // example how to build a class to process huge XML stream of records
    class MyXomAnalyzer extends XomAnalyzer {

        public void initRecord(String name) {

            // just start a new record
            rsId = "rs";
            het = null;  // avg heterozygosity score
            stdErr = null; // standard error for het score
            chr = "?"; // chromosome
            mafFreq = null;
            mafSampleSize = null;
            mafAllele = null;
            molType = null;
            genotype = null;
            hetType = null;
            exemplarSS = "";
            ancestralAllele = "";
            exemplarSSOrient = "";
            contigOrient = "";
            clinicalSignificance = "";
            dbSnpAllele = "";
            refAllele = "";

            posList.clear();
            fxnClassList.clear();
            mapLocOrient.clear();
            hgvsNames.clear();
            mergeHistory.clear();
        }

        DbSnpDao dao;
        BufferedWriter dumper;
        String source;
        int mapKey;
        String groupLabel;
        List<DbSnp> dbSnpList = new ArrayList<>();
        String refChromosome;
        boolean withClinicalSignificance;

        public void init(BufferedWriter dumper, DbSnpDao dao, String source, int mapKey, String groupLabel,
                         String refChr, boolean withClinicalSignificance) throws Exception {
            this.source = source;
            this.dumper = dumper;
            this.dao = dao;
            this.mapKey = mapKey;
            this.groupLabel = groupLabel;
            this.refChromosome = refChr;
            this.withClinicalSignificance = withClinicalSignificance;
        }


        public void flush() throws Exception {
            // writes all records to database
            dao.insert(dbSnpList);
            // clear the list
            dbSnpList.clear();
        }

        // RS record properties
        String dbSnpBuild;
        String genomeBuild;
        String rsId;
        String snpClass; // "snp", ...
        String molType; // "genomic", ...
        String genotype; // "true", ...
        Double het;  // avg heterozygosity score
        Double stdErr; // standard error for het score
        String hetType; // est or obs
        String chr; // chromosome
        String dbSnpAllele;
        String refAllele;
        Double mafFreq;
        Integer mafSampleSize;
        String mafAllele;
        String exemplarSS;
        String ancestralAllele;
        String exemplarSSOrient;
        String contigOrient;
        String clinicalSignificance;

        List<Integer> posList = new ArrayList<>(); // position on chromosome
        List<String> fxnClassList = new ArrayList<>();
        List<String> mapLocOrient = new ArrayList<>();
        List<String> hgvsNames = new ArrayList<>();
        Map<String,Integer> mergeHistory = new HashMap<>();

        private String changeOrientation(String nucleotides) {

            if( nucleotides==null )
                return null;

            // ensure that all letters are among 'A','C','G','T'
            // or we do not do the conversion
            boolean skipConversion = false;
            for( int i=0; i<nucleotides.length(); i++ ) {
                char nuc = nucleotides.charAt(i);
                if( Character.isLetter(nuc) ) {
                    if( nuc!='A' && nuc!='C' && nuc!='G' && nuc!='T' && nuc!='N' ) {
                        skipConversion = true;
                        break;
                    }
                }
            }
            if( skipConversion)
                return nucleotides;

            StringBuilder buf = new StringBuilder(nucleotides);
            for( int i=0; i<buf.length(); i++ ) {
                char nuc = buf.charAt(i);
                if( nuc=='A' )
                    buf.setCharAt(i, 'T');
                else if( nuc=='T' )
                    buf.setCharAt(i, 'A');
                else if( nuc=='C' )
                    buf.setCharAt(i, 'G');
                else if( nuc=='G' )
                    buf.setCharAt(i, 'C');
            }
            return buf.toString();
        }

        private String handleOrientation(String mapLocOrient, String nuc) {

            // analysis of real data revealed that when mapLocOrient is "reverse", we need to change
            // maf_allele
            if( mapLocOrient.equals("reverse") )
                return changeOrientation(nuc);
            else
                return nuc;
        }

        void insert(int pos, String fxnClass, String mapLocOrient) throws Exception {

            String mafAllele = handleOrientation(mapLocOrient, this.mafAllele);

            boolean complexCase = false;
            if( !dbSnpAllele.matches("^[-ACGT/]+$") ) {
                complexCase = true;
                //System.out.println("complex allele "+rsId+" "+dbSnpAllele);
            }

            // in source data allele is like that: "A/G" or "-/TGGG"
            // instead we create two entries, one with allele "A", another with allele "G" (or "-", "TGGG")
            String[] alleles = dbSnpAllele.split("[/]");

            // handle repeats -- to detect them to be repeated sequence is in parentheses followed by repeat number;
            // the other alleles contain only repeats
            if( complexCase ) {
                // expand repeated sequence, f.e
                // {"(ACG)2/3/4"} into {"(ACG)2", "(ACG)3", "(ACG)4"}
                int pos1 = alleles[0].indexOf('(');
                int pos2 = alleles[0].indexOf(')');
                String repeat = alleles[0].substring(pos2+1);
                if( repeat.matches("^[0-9]+$") && pos1==0 && pos2>0 ) {
                    String seq = alleles[0].substring(0, pos2+1);
                    for( int i=1; i<alleles.length; i++ ) {
                        if( alleles[i].matches("^[0-9]+$") ) {
                            alleles[i] = seq + alleles[i];
                        }
                        else {
                            //System.out.println("unexpected complex case 1");
                        }
                    }
                }
                else if( pos2+1 == alleles[0].length() ) {
                    // not a complex case -- regular allele processing
                }
                else {
                    //System.out.println("unexpected complex case 2");
                }
            }

            if( !this.snpClass.equals("snp") ) {
                //System.out.println("unsupported");

            } else {
                refAllele = getRefNucleotide(this.chr, pos, mapKey);
                for (String allele : alleles) {
                    String plusStrandedAllele = handleOrientation(mapLocOrient, allele);
                    // do not insert ref allele
                    if (!plusStrandedAllele.equals(refAllele)) {
                        doInsert(pos, fxnClass, plusStrandedAllele, mafAllele);
                    }
                }
            }
        }

        void doInsert(int pos, String fxnClass, String allele, String mafAllele) throws Exception {

            // create new dbSnp object
            DbSnp dbSnp = new DbSnp();
            dbSnp.setAvgHetroScore(het);
            dbSnp.setChromosome(chr);
            dbSnp.setMapKey(mapKey);
            dbSnp.setPosition(pos);
            dbSnp.setSnpName(rsId);
            dbSnp.setSource(source);
            dbSnp.setStdError(stdErr);
            dbSnp.setAllele(allele);
            dbSnp.setMafFrequency(mafFreq);
            dbSnp.setMafSampleSize(mafSampleSize);
            dbSnp.setMafAllele(mafAllele);
            dbSnp.setSnpClass(snpClass);
            dbSnp.setMolType(molType);
            dbSnp.setGenotype(genotype);
            dbSnp.setHetroType(hetType);
            dbSnp.setMapLocCount(posList.size());
            dbSnp.setFunctionClass(fxnClass);
            dbSnp.setAncestralAllele(ancestralAllele);
            dbSnp.setRefAllele(refAllele);
            dbSnp.setClinicalSignificance(clinicalSignificance);
            dbSnp.setHgvs(new ArrayList<>(hgvsNames)); // make a copy of data
            dbSnp.setMergeHistory(new HashMap<>(mergeHistory));// make a copy of data

            // add dbSnp object to the list of objects pending for insert into db
            dbSnpList.add(dbSnp);

            // dump the data
            if( dumper!=null ) {
                String builder = rsId + '\t' +
                        chr + '\t' +
                        pos + '\t' +
                        hetType + '\t' +
                        het + '\t' +
                        stdErr + '\t' +
                        snpClass + '\t' +
                        molType + '\t' +
                        genotype + '\t' +
                        dbSnpBuild + '\t' +
                        genomeBuild + '\t' +
                        allele + '\t' +
                        mafFreq + '\t' +
                        mafSampleSize + '\t' +
                        mafAllele + '\t' +
                        posList.size() + '\t' +
                        fxnClass + '\t' +
                        ancestralAllele + '\t' +
                        clinicalSignificance + '\t' +
                        '\n';
                dumper.write(builder);
            }

            // dump the data into database if buffer full
            if( dbSnpList.size()>=dao.getBatchSize() ) {
                flush();
            }
        }

        // entire record has been parsed
        public Element parseRecord(Element element) {

            // process only Rs elements
            if( !element.getLocalName().equals("Rs"))
                return null;

            try {
                rsId = "rs"+element.getAttributeValue("rsId");

                if( withClinicalSignificance && Utils.isStringEmpty(clinicalSignificance) ) {
                    skippedDbSnpEntriesWithoutClinicalSignificance++;
                    return null;
                }

                snpClass = element.getAttributeValue("snpClass");
                molType = element.getAttributeValue("molType");
                genotype = element.getAttributeValue("genotype");

                if( !chr.equals(refChromosome) ) {
                    System.out.println("  problematic chromosome: expected ["+refChromosome+"], was ["+chr+"] for "+rsId);
                }
                else {
                    // insert into db
                    if( snpClass.equals("snp") ) {
                        for (int i = 0; i < posList.size(); i++) {
                            insert(posList.get(i), fxnClassList.get(i), mapLocOrient.get(i));
                        }
                    } else {
                        skippedDbSnpEntries++;
                    }
                }
            }
            catch(Exception e) {
                System.out.println("rs="+rsId);
                e.printStackTrace();
            }
            return null; // discard the element from resulting document (a must if your XML stream is huge)
        }

        public Element parseSubrecord(Element element) {

            try {

            // load heterozygosity score and std error
            switch (element.getLocalName()) {
                case "Het":
                    het = Double.parseDouble(element.getAttributeValue("value"));
                    stdErr = Double.parseDouble(element.getAttributeValue("stdError"));
                    hetType = element.getAttributeValue("type");
                    break;
                case "Sequence":

                    // just read attribute of exemplarSS
                    exemplarSS = element.getAttributeValue("exemplarSs");

                    ancestralAllele = element.getAttributeValue("ancestralAllele");

                    for (int seqEle = 0; seqEle < element.getChildElements().size(); seqEle++) {
                        if (element.getChildElements().get(seqEle).getLocalName().equals("Observed")) {

                            //set allele in new rsID object
                            dbSnpAllele = element.getChildElements().get(seqEle).getValue();
                        }
                    }
                    break;
                case "Ss":
                    // look for exemplarSS
                    String ssId = element.getAttributeValue("ssId");
                    if (ssId.equals(exemplarSS)) {
                        exemplarSSOrient = element.getAttributeValue("orient");
                    }
                    break;
                case "Assembly":
                    // process only reference assembly
                    String groupLabel = element.getAttributeValue("groupLabel");

                    if (groupLabel != null && groupLabel.equals(this.groupLabel)) {
                        dbSnpBuild = element.getAttributeValue("dbSnpBuild");
                        genomeBuild = element.getAttributeValue("genomeBuild");

                        // process Component subelement
                        Elements components = element.getChildElements();
                        int componentCount = 0;
                        for (int i = 0; i < components.size(); i++) {
                            Element component = components.get(i);
                            if (!component.getLocalName().equals("Component"))
                                continue;

                            // there must be a non-empty start
                            String start = component.getAttributeValue("start");
                            if (start == null || start.equals(""))
                                continue;
                            componentCount++;
                            if (componentCount > 1) {
                                //System.out.println("fatal: multiple Components");
                            }

                            chr = component.getAttributeValue("chromosome");
                            int contigStart = Integer.parseInt(start);
                            String strand = component.getAttributeValue("orientation");
                            if (!strand.equals("fwd")) {
                                int contigEnd = Integer.parseInt(component.getAttributeValue("end"));
                                logger.info("contig " + contigStart + "-" + contigEnd);
                            }
                            contigOrient = strand;

                            Elements mapLocs = component.getChildElements();

                            int mapLocCount = 0;
                            for (int j = 0; j < mapLocs.size(); j++) {
                                Element mapLoc = mapLocs.get(j);
                                if (!mapLoc.getLocalName().equals("MapLoc"))
                                    continue;
                                mapLocCount++;
                                if (mapLocCount > 1) {
                                    logger.info("fatal: multiple MapLocs");
                                }

                                mapLocOrient.add(mapLoc.getAttributeValue("orient"));

                                // location within config
                                int asnFrom = Integer.parseInt(mapLoc.getAttributeValue("asnFrom"));
                                int asnTo = Integer.parseInt(mapLoc.getAttributeValue("asnTo"));
                                if (asnFrom > asnTo) {
                                    logger.error("fatal: asnFrom > asnTo");
                                }
                                posList.add(contigStart + asnFrom + 1);

                                // parse <FxnSet> elements, possibly multiple, and extract attribute 'fxnClass'
                                Set<String> fxnClasses = new HashSet<String>();
                                Elements fxnSets = mapLoc.getChildElements();

                                for (int f = 0; f < fxnSets.size(); f++) {
                                    Element fxnSet = fxnSets.get(f);
                                    if (!fxnSet.getLocalName().equals("FxnSet"))
                                        continue;

                                    fxnClasses.add(fxnSet.getAttributeValue("fxnClass") + " " + fxnSet.getAttributeValue("mrnaAcc"));
                                }

                                String fxnClassesStr = Utils.concatenate(fxnClasses, ", ");
                                // maximum field length is 4000 -- truncate longer contents
                                if (fxnClassesStr.length() > 4000) {
                                    int pos = fxnClassesStr.lastIndexOf(", ", 4000 - 4);
                                    if (pos > 0)
                                        fxnClassesStr = fxnClassesStr.substring(0, pos) + " ...";
                                    else
                                        fxnClassesStr = fxnClassesStr.substring(0, 4000 - 4) + " ...";
                                }
                                fxnClassList.add(fxnClassesStr);
                            }
                        }
                    }
                    break;
                case "Frequency":

                    this.mafFreq = Double.parseDouble(element.getAttributeValue("freq"));
                    this.mafSampleSize = Integer.parseInt(element.getAttributeValue("sampleSize"));
                    this.mafAllele = element.getAttributeValue("allele");
                    break;
                case "Phenotype":

                    Elements css = element.getChildElements();

                    int csCount = 0;
                    for (int j = 0; j < css.size(); j++) {
                        Element cs = css.get(j);
                        if (!cs.getLocalName().equals("ClinicalSignificance"))
                            continue;
                        if (csCount == 0)
                            this.clinicalSignificance = cs.getValue();
                        else
                            this.clinicalSignificance += "; " + cs.getValue();
                        csCount++;
                    }
                    //if( csCount>0 ) {
                    //    System.out.println("ClinicalSignificance for "+this.rsId+" is "+clinicalSignificance);
                    //}
                    break;
                case "hgvs":

                    this.hgvsNames.add(element.getValue());
                    break;
                case "MergeHistory":
                    String rsId = element.getAttributeValue("rsId");
                    String buildId = element.getAttributeValue("buildId");
                    this.mergeHistory.put("rs" + rsId, Integer.parseInt(buildId));
                    break;
            }

            }
            catch(Exception e) {
                System.out.println("rs="+rsId);
                e.printStackTrace();
            }
            return null;
        }
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}