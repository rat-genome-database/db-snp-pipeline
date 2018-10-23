package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.xml.XomAnalyzer;
import nu.xom.Element;
import nu.xom.Elements;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * Creates a set of gff3 files for loading into GBrowse or JBrowse given xml ftp files from NCBI DB_SNP database.
 */
public class DbSnpGff3Creator {
    String newpathlog;
    String filesDirectory;
    String newrecordTag;
    String newprefix;
    String newnslink;
    String newchromosome;
    int newstart;
    String newpathgff3;
    PrintWriter gffFileRGSC;
    PrintWriter gffFileCelera;
    int Ccounter = 1;
    int Rcounter = 1;

    Map<String,Map<String,String>> rsMapCelera= new HashMap<>(); //rs vs text for Celera
    Map<String,Map<String,String>> rsMapRef=new HashMap<>();   //rs vs text for ref assembly v6.0
    private String version;

    static public void main(String[] args) throws Exception{

        XmlBeanFactory bf=new XmlBeanFactory(new FileSystemResource("properties/AppConfigure.xml"));
        DbSnpGff3Creator instance = (DbSnpGff3Creator) (bf.getBean("gff3Creator"));

        // do NOT generate gff3 files for Celera
        boolean generateCeleraFiles = false;

        // cmd line arg: -inputDir=xxx
        //   directory where ds_chrXXX.xml.gz and gt_chrXXX.xml.gz files are located
        String inputDir = null;
        for( String arg: args ) {
            if( arg.startsWith("-inputDir=") ) {
                inputDir = arg.substring(10);
            }
        }

        try {
            instance.run(inputDir, generateCeleraFiles);

        }catch (Exception run){
            run.printStackTrace();
        }
    }

    void run(String inputDir, boolean generateCeleraFiles) throws Exception {

        System.out.println(getVersion());

        setNewpathgff3("data/");

        if( !inputDir.endsWith("/") && !inputDir.endsWith("\\") ) {
            inputDir += "/";
        }
        setFilesDirectory(inputDir);

        // iterate over ds_chr*.xml.gz files to find out the chromosomes available
        List<String> chromosomes = new ArrayList<>();
        for( File f: new File(inputDir).listFiles() ) {
            // file name must start with 'ds_chr' and end with '.xml.gz'
            if( f.isDirectory() ) {
                continue;
            }
            String fname = f.getName();
            if( fname.startsWith("ds_ch") && fname.endsWith(".xml.gz") ) {
                String chr = fname.substring(5, fname.length()-7);
                if( chr.length()<=2 && chr.compareToIgnoreCase("Un")!=0 ) {
                    chromosomes.add(chr);
                }
            }
        }
        Collections.shuffle(chromosomes);

        for( String chr: chromosomes ) {
            processChr(chr, generateCeleraFiles);
        }
    }

    void processChr(String chr, boolean generateCeleraFiles) throws Exception {

        String snpXMLFileName = getFileName(chr, "ds_ch");
        String genotypeFileName = getFileName(chr, "gt_chr");

        System.out.println(" Starting Chr"+chr);

        Map<String,Map<String,Map<String,String>>> putIntoMap=new HashMap<>();
        //initialising new hash maps each for a celera assembly and Rnor_6.0
        putIntoMap.put("Rn_Celera",rsMapCelera);
        putIntoMap.put("Rnor_6.0",rsMapRef);

        //map the Strains for each population ID
        Map<String,String> popIdvsStrain = createPopIDvsStrainMap(genotypeFileName);
        Map<String,Set<String>> rsIdToPopIdsMap = createRsIdToPopIdsMap(genotypeFileName);


        File reader = new File(snpXMLFileName);
        System.out.println("this is the current counter number" + Rcounter);  //each dbsnp id gets its own custom name with the Rcounter as the serial number.
        //creating new gff3 files for each assembly and each chromosome number
        gffFileRGSC=new PrintWriter(getNewpathgff3()+"dbSNP_"+"Rnor_6.0_chr"+chr+".gff3");
        gffFileRGSC.println("##gff-version 3");

        if( generateCeleraFiles ) {
            gffFileCelera = new PrintWriter(getNewpathgff3() + "dbSNP_" + "Celera_chr" + chr + ".gff3");
            gffFileCelera.println("##gff-version 3");
        }

        //creating new instance of the XOMAnalyser parser that does the parsing of each node(corresponding to each rs ID) in the xml file
        MyXomAnalyzer xom = new MyXomAnalyzer(popIdvsStrain, rsIdToPopIdsMap, putIntoMap, chr);
        //starting process for each "node" in the chromosome xml file.
        xom.setValidate(false);
        xom.parse(reader);


        System.out.println("---------------Chromosome" + chr + " done-------------------------");
        gffFileRGSC.close();
        if( gffFileCelera!=null ) {
            gffFileCelera.close();
        }
    }

    String getFileName(String chrNum, String prefix) {
        return getFilesDirectory()+prefix + chrNum + ".xml.gz";
    }

    // map of popId -> locPopId
    public static Map<String,String> createPopIDvsStrainMap(String genotypeFileName)throws Exception{

        final Map<String,String> gtMap= new HashMap<>();

        XomAnalyzer analyzer = new XomAnalyzer() {

            public Element parseRecord(Element element) {

                if( element.getLocalName().equals("Population") ) {
                    String gtPopId =  element.getAttributeValue("popId");
                    String gtStrain =  element.getAttributeValue("locPopId");
                    //add strains for each population ID
                    gtMap.put(gtPopId,gtStrain);
                }
                return null;
            }
        };

        analyzer.setValidate(false);
        analyzer.parse(new File(genotypeFileName));

        return gtMap;
    }

    // map of rsId -> Set<popId>
    public static Map<String,Set<String>> createRsIdToPopIdsMap(String genotypeFileName)throws Exception{

        final Map<String,Set<String>> gtMap= new HashMap<>();

        XomAnalyzer analyzer = new XomAnalyzer() {

            public Element parseRecord(Element element) {

                if( element.getLocalName().equals("SnpInfo") ) {
                    String rsId = element.getAttributeValue("rsId");
                    Set<String> popIds = new HashSet<>();
                    if( gtMap.put(rsId, popIds)!=null )
                        System.out.println("unexpected file format error");

                    Elements snpInfochildren = element.getChildElements();
                    for (int i=0; i<snpInfochildren.size(); i++) {
                        Element snpInfoChild = snpInfochildren.get(i);
                        if( snpInfoChild.getLocalName().equals("SsInfo") ){
                            Elements snpInfoChildEle = snpInfoChild.getChildElements();
                            for (int ii=0;ii<snpInfoChildEle.size(); ii++) {
                                Element ssInfoChild = snpInfoChildEle.get(ii);
                                if(ssInfoChild.getLocalName().equals("ByPop")){
                                    String popId = ssInfoChild.getAttributeValue("popId");
                                    popIds.add(popId);
                                }
                            }
                        }
                    }
                }
                return null;
            }
        };

        analyzer.setValidate(false);
        analyzer.parse(new File(genotypeFileName));

        return gtMap;

    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    // example how to build a class to process huge XML stream of records
    class MyXomAnalyzer extends XomAnalyzer {

        Map<String,String> populationIDVsStrain;
        Map<String,Set<String>> rsIdToPopIdsMap;
        Map<String,Map<String,Map<String,String>>>assemblyMap=new HashMap<>();
        String chromosomeNum;

        public MyXomAnalyzer(Map<String, String> popIdvsStrain,
                             Map<String, Set<String>> rsIdToPopIdsMap,
                             Map<String, Map<String, Map<String, String>>> putIntoMap, String InputChrNum) {
            this.populationIDVsStrain = popIdvsStrain;
            this.rsIdToPopIdsMap = rsIdToPopIdsMap;
            this.assemblyMap = putIntoMap;
            this.chromosomeNum = InputChrNum;
        }

        public void initRecord(String name) {

            // just start a new record
            //System.out.println("Start processing record:" + name);
        }

        // entire record has been parsed
        public Element parseRecord(Element node){

            if(!node.getLocalName().equals("Rs")) {  //<Rs bitField="050100800001000000000100" molType="cDNA" rsId="8156416" snpClass="snp" snpType="notwithdrawn">
                return null;
            }

            String rsNum =  node.getAttributeValue("rsId");
            System.out.println("RSID:" + rsNum);

            Set<String> strains = rsIdToPopIdsMap.get(rsNum);//strain Info in a set
            String strainNames = strains==null ? "" : strains.toString();
            strainNames = strainNames.replaceAll("[ \\[\\]]","");

            Elements children = node.getChildElements(); //children of particular rs
            String start = "";
            String chromosome = "";
            Map<String,Integer> validationCounter =  new HashMap<>();
            int validationCount = 0;
            String validatedBy ="";
            String allele = null;

            //loop begins for each Rs number
            for (int ja=0; ja<children.size();ja++) {
                Element child = children.get(ja);
                // parse SS
                if(child.getLocalName().equals("Ss")){
                    parseSS(child, validationCounter);
                    continue;
                }

                if(child.getLocalName().equals("Sequence")){
                    allele = parseSequence(child);
                    continue;
                }

                // parse Assembly
                if( child.getLocalName().equals("Assembly") ){
                    String assembly = child.getAttributeValue("groupLabel");
                    if(assembly==null)
                        continue;

                    Elements assemblyList = child.getChildElements();
                    List<String> startarr= new ArrayList<>();
                    List<String> chrarr= new ArrayList<>();
                    for (int jj=0;jj<assemblyList.size();jj++) {
                        Element assemblyFeature = assemblyList.get(jj);
                        if(assemblyFeature.getLocalName().equals("Component")){
                            Elements componentList = assemblyFeature.getChildElements();
                            for (int k=0;k<componentList.size();k++) {
                                Element componentFeature = componentList.get(k);
                                if(componentFeature.getLocalName().equals("MapLoc")){

                                    String type = "";
                                    String readingFrame = "";
                                    chromosome = assemblyFeature.getAttributeValue("chromosome");
                                    chrarr.add(chromosome);
                                    start = assemblyFeature.getAttributeValue("start");
                                    int startbp = 0;
                                    if (start!=null && !start.equals("")){
                                        startbp = Integer.valueOf(start);
                                    }
                                    String asnFrom = componentFeature.getAttributeValue("asnFrom");
                                    int asnFrombp=Integer.parseInt(asnFrom);
                                    startbp = startbp + asnFrombp + 1;
                                    start = "" + startbp;
                                    startarr.add(start);
                                    Elements mapLocList = componentFeature.getChildElements();
                                    int fxnSetCounter=0;
                                    List<String> typearr= new ArrayList<>();
                                    for (int kk=0; kk<mapLocList.size(); kk++) {
                                        Element mapLocFeature = mapLocList.get(kk);
                                        if(mapLocFeature.getLocalName().equals("FxnSet")){
                                            fxnSetCounter++;
                                            String fxnClass = mapLocFeature.getAttributeValue("fxnClass");
                                            if(fxnClass!=null && !fxnClass.equals("")){
                                                type = fxnClass;
                                            }
                                            if(!type.equals("")){
                                                if(!typearr.contains(type)){
                                                    typearr.add(type);
                                                }
                                            }
                                            readingFrame = mapLocFeature.getAttributeValue("readingFrame");
                                        }
                                    }
                                    Map<String,String> temp=new HashMap<>();
                                    assemblyMap.get(assembly).put((rsNum+"Chr"+chromosome+"start"+start),temp);
                                    if(typearr.size()>1){
                                        for (String aType: typearr) {
                                            if (!aType.equals("reference")) {
                                                if (assemblyMap.get(assembly).containsKey(rsNum + "Chr" + chromosome + "start" + start)) {
                                                    temp = assemblyMap.get(assembly).get(rsNum + "Chr" + chromosome + "start" + start);
                                                }
                                                String typeString;
                                                if (temp.containsKey("type")) {
                                                    typeString = temp.get("type") + "," + aType;
                                                } else {
                                                    typeString = aType;
                                                }
                                                temp.put("type", typeString);
                                                assemblyMap.get(assembly).put((rsNum + "Chr" + chromosome + "start" + start), temp);
                                            }
                                        }
                                    }
                                    else if(typearr.size() == 1){
                                        if(typearr.get(0) == null){}else if(!typearr.get(0).equals("")){
                                            if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                                temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                                            }
                                            String typeString = typearr.get(0);
                                            temp.put("type",typeString);
                                            assemblyMap.get(assembly).put((rsNum+"Chr"+chromosome+"start"+start),temp);
                                        }
                                    }

                                    if(readingFrame!=null && !readingFrame.equals("")){
                                        if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                            temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                                        }
                                        temp.put("ReadingFrame",readingFrame);
                                        assemblyMap.get(assembly).put((rsNum+"Chr"+chromosome+"start"+start),temp);
                                    }

                                    if (allele != null && !allele.isEmpty() ){
                                        if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                            temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                                        }
                                        temp.put("allele",allele);
                                        assemblyMap.get(assembly).put((rsNum+"Chr"+chromosome+"start"+start),temp);
                                    }

                                    if (!strainNames.equals("")){
                                        if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                            temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                                        }
                                        temp.put("strain",strainNames);
                                        assemblyMap.get(assembly).put((rsNum+"Chr"+chromosome+"start"+start),temp);
                                    }
                                } //end of MapLoc
                            }
                        } //end of Component
                        else if(assemblyFeature.getLocalName().equals("SnpStat")){
                            String mapWeight = assemblyFeature.getAttributeValue("mapWeight");
                            Map<String,String> temp=new HashMap<>();
                            if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                            }
                            temp.put("mapWeight",mapWeight);
                            for(int j=0;j<startarr.size();j++){
                                assemblyMap.get(assembly).put((rsNum+"Chr"+chrarr.get(j)+"start"+startarr.get(j)),temp);
                            }
                        }

                        for (Map.Entry<String, Integer> entry: validationCounter.entrySet()) {
                            int y = entry.getValue();
                            if (y > validationCount) {
                                validationCount = y;
                                validatedBy = entry.getKey();
                            }
                        }

                        if (validatedBy!=null && !validatedBy.equals("")){
                            Map<String,String> temp=new HashMap<>();
                            if(assemblyMap.get(assembly).containsKey(rsNum+"Chr"+chromosome+"start"+start)){
                                temp = assemblyMap.get(assembly).get(rsNum+"Chr"+chromosome+"start"+start);
                            }
                            temp.put("validated",validatedBy);
                            for(int j=0;j<startarr.size();j++){
                                assemblyMap.get(assembly).put((rsNum+"Chr"+chrarr.get(j)+"start"+startarr.get(j)),temp);
                            }
                        }
                    }//end of for loop
                }//end of Assembly
                System.out.println("done with Rs" + rsNum);

                //function called here to print each record.
                // in the end, maps has to be  cleared in order to prevent appending the next record details into the same map
                printgff3ref(assemblyMap.get("Rnor_6.0"));
                rsMapRef.clear();

                printgff3celera(assemblyMap.get("Rn_Celera"));
                rsMapCelera.clear();
            }//end of each Rs number


            return null; // discard the element from resulting document (a must if your XML stream is huge)
        }

        void parseSS(Element child, Map<String,Integer> validationCounter) {
            String validated = child.getAttributeValue("validated");
            if(validated!=null && !validated.equals("")){
                if(validationCounter.containsKey(validated)){
                    validationCounter.put(validated,1+validationCounter.get(validated));
                }
                else{
                    validationCounter.put(validated,1);
                }
            }
        }

        // look for child element '<Observed>A/G</Observed>'
        String parseSequence(Element el) {
            Elements elements = el.getChildElements();
            for( int i=0; i<elements.size(); i++ ) {
                Element el2 = elements.get(i);
                if( el2.getLocalName().equals("Observed") ) {
                    return el2.getValue();
                }
            }
            return "";
        }

        public void printgff3celera(Map<String,Map<String,String>> textMap)  {

            if( gffFileCelera==null ) {
                return;
            }

            if( textMap.size()>1 ){
                System.out.println("celera maps:" + textMap.keySet());
                for (Map.Entry<String, Map<String, String>> entry : textMap.entrySet()) {
                    Object key = entry.getKey();
                    String arr[] = key.toString().split("Chr");
                    String arr2[] = arr[1].split("start");
                    String chromosome = arr2[0];
                    String start = arr2[1];
                    String rsNumber = arr[0];
                    Map<String, String> snpAttributes = entry.getValue();
                    if (snpAttributes.containsKey("type")) {
                        String types[] = snpAttributes.get("type").split(",");
                        for (String type : types) {
                            printgff3Line_more_thanOne_Hit(snpAttributes, gffFileCelera, chromosome, start, rsNumber, type, Ccounter);
                            Ccounter++;
                        }
                    } else {

                        printgff3Line_more_thanOne_Hit(snpAttributes, gffFileCelera, chromosome, start, rsNumber, "", Ccounter);
                        Ccounter++;
                    }
                }
            }else{
                for (Map.Entry<String, Map<String, String>> entry: textMap.entrySet()) {
                    Object key = entry.getKey();
                    String arr[] = key.toString().split("Chr");
                    String arr2[] = arr[1].split("start");
                    String chromosome = arr2[0];
                    String start = arr2[1];
                    String rsNumber = arr[0];
                    Map<String, String> snpAttributes = entry.getValue();
                    if (snpAttributes.containsKey("type")) {
                        String types[] = snpAttributes.get("type").split(",");
                        for (String type : types) {
                            printgff3Line(snpAttributes, gffFileCelera, chromosome, start, rsNumber, type, Ccounter);
                            Ccounter++;
                        }
                    } else {

                        printgff3Line(snpAttributes, gffFileCelera, chromosome, start, rsNumber, "", Ccounter);
                        Ccounter++;
                    }
                }
            }

        }

        public void printgff3ref(Map<String,Map<String,String>> textMap) {

            if(textMap.size()>1){
                System.out.println("ref maps:" + textMap.keySet());
                for (Map.Entry<String, Map<String, String>> entry: textMap.entrySet()) {
                    Object key = entry.getKey();
                    String arr[] = key.toString().split("Chr");
                    String arr2[] = arr[1].split("start");
                    String chromosome = arr2[0];
                    String start = arr2[1];
                    String rsNumber = arr[0];
                    Map<String, String> snpAttributes = textMap.get(key);
                    if (snpAttributes.containsKey("type")) {
                        String types[] = snpAttributes.get("type").split(",");
                        for (String type: types) {
                            printgff3Line_more_thanOne_Hit(snpAttributes, gffFileRGSC, chromosome, start, rsNumber, type, Rcounter);

                            Rcounter++;
                        }
                    } else {
                        printgff3Line_more_thanOne_Hit(snpAttributes, gffFileRGSC, chromosome, start, rsNumber, "", Rcounter);

                        Rcounter++;
                    }
                }
            }
            else{
                for (Map.Entry<String, Map<String, String>> entry: textMap.entrySet()) {
                    Object key = entry.getKey();
                    String arr[] = key.toString().split("Chr");
                    String arr2[] = arr[1].split("start");
                    String chromosome = arr2[0];
                    String start = arr2[1];
                    String rsNumber = arr[0];
                    Map<String, String> snpAttributes = textMap.get(key);
                    if (snpAttributes.containsKey("type")) {
                        String types[] = snpAttributes.get("type").split(",");
                        for (String type : types) {
                            printgff3Line(snpAttributes, gffFileRGSC, chromosome, start, rsNumber, type, Rcounter);

                            Rcounter++;
                        }
                    } else {
                        printgff3Line(snpAttributes, gffFileRGSC, chromosome, start, rsNumber, "", Rcounter);

                        Rcounter++;
                    }
                }
            }

        }

        public void printgff3Line(Map<String,String> attributes, PrintWriter gffFile,String chr, String start, String rs, String type, int tableKey) {
            gffFile.print("Chr"+chr+"\tdbSNP_uniqueInContig\tSNP\t"+start+"\t"+start+"\t.\t.\t.\tID=rs"+rs+"_"+tableKey+";Name=ratdbsnp"+tableKey+"chr"+chr+";Alias=rs"+rs);
            if( !type.isEmpty() ){
                gffFile.print(";type="+type);
                System.out.println("type printing" + type);
            }
            if(attributes.containsKey("allele")){
                gffFile.print(";allele="+attributes.get("allele"));
            }
            if(attributes.containsKey("mapWeight")){
                gffFile.print(";mapweight="+attributes.get("mapWeight"));
            }
            if(attributes.containsKey("strain")){
                gffFile.print(";strain="+attributes.get("strain"));
            }
            if(attributes.containsKey("ReadingFrame")){
                gffFile.print(";readingFrame="+attributes.get("ReadingFrame"));
            }
            if(attributes.containsKey("validated")){
                gffFile.print(";validated="+attributes.get("validated"));
            }
            gffFile.print("\n");
        }

        public void printgff3Line_more_thanOne_Hit(Map<String,String> attributes, PrintWriter gffFile,String chr, String start, String rs, String type, int tableKey) {
            gffFile.print("Chr"+chr+"\tdbSNP_moreThanOneHit\tSNP\t"+start+"\t"+start+"\t.\t.\t.\tID=rs"+rs+"_"+tableKey+";Name=ratdbsnp"+tableKey+"chr"+chr+";Alias=rs"+rs);
            if( !type.isEmpty() ){
                gffFile.print(";type="+type);
                System.out.println("type printing" + type);
            }
            if(attributes.containsKey("allele")){
                gffFile.print(";allele="+attributes.get("allele"));
            }
            if(attributes.containsKey("mapWeight")){
                gffFile.print(";mapweight="+attributes.get("mapWeight"));
            }
            if(attributes.containsKey("strain")){
                gffFile.print(";strain="+attributes.get("strain"));
            }
            if(attributes.containsKey("ReadingFrame")){
                gffFile.print(";readingFrame="+attributes.get("ReadingFrame"));
            }
            if(attributes.containsKey("validated")){
                gffFile.print(";validated="+attributes.get("validated"));
            }
            gffFile.print("\n");
        }
    }


    public String getNewpathlog() {
        return newpathlog;
    }

    public void setNewpathlog(String newpathlog) {
        this.newpathlog = newpathlog;
    }

    public String getFilesDirectory() {
        return filesDirectory;
    }

    public void setFilesDirectory(String filesDir) {
        this.filesDirectory = filesDir;
    }

    public String getNewrecordTag() {
        return newrecordTag;
    }

    public void setNewrecordTag(String newrecordTag) {
        this.newrecordTag = newrecordTag;
    }

    public String getNewprefix() {
        return newprefix;
    }

    public void setNewprefix(String newprefix) {
        this.newprefix = newprefix;
    }

    public String getNewnslink() {
        return newnslink;
    }

    public void setNewnslink(String newnslink) {
        this.newnslink = newnslink;
    }

    public String getNewchromosome() {
        return newchromosome;
    }

    public void setNewchromosome(String newchromosome) {
        this.newchromosome = newchromosome;
    }

    public int getNewstart() {
        return newstart;
    }

    public void setNewstart(int newstart) {
        this.newstart = newstart;
    }

    public String getNewpathgff3() {
        return newpathgff3;
    }

    public void setNewpathgff3(String newpathgff3) {
        this.newpathgff3 = newpathgff3;
    }
}
