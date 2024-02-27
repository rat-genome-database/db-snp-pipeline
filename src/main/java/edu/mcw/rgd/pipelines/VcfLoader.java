package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.process.Utils;

import java.io.BufferedReader;
import java.util.HashMap;

public class VcfLoader {

    DbSnpDao dao = new DbSnpDao();

    public void run( String inputFile, String build, int mapKey, DbSnpLoader loader ) throws Exception {

        BufferedReader in = Utils.openReader(inputFile);

        java.util.Map<String,String> refSeqToChrMap = dao.getRefSeqToChrMap(mapKey);
        java.util.Map<String,Integer> varTypeMap = new HashMap<>();

        String line;
        while( (line=in.readLine())!=null ) {

            // skip header lines
            if( line.startsWith("#") ) {
                continue;
            }

            String[] cols = line.split("[\\t]", -1);
            if( cols.length < 8 ) {
                continue;
            }

            String refseqChr = cols[0];
            String chr = refSeqToChrMap.get(refseqChr);
            if( chr == null ) {
                System.out.println("*** unknown RefSeq chromosome "+refseqChr);
            }

            int pos = Integer.parseInt(cols[1]);
            String rsId = cols[2];
            String ref = cols[3];
            String alleles = cols[4];
            String info = cols[7];

            String varType = parseVarType(info, varTypeMap);
            if( varType == null ) {
                System.out.println(" null varType");
            }
            String[] allele = alleles.split(",");

            for( String alleleNuc: allele ) {

                DbSnp rec = new DbSnp();
                rec.setAllele(alleleNuc);
                rec.setChromosome(chr);
                rec.setMapKey(mapKey);
                rec.setPosition(pos);
                rec.setRefAllele(ref);
                rec.setSnpClass(varType);
                rec.setSnpName(rsId);
                rec.setSource(build);
                loader.insertToDb(rec);
            }

        }

        in.close();

        loader.insertToDb(null);// signal end of stream of DbSnp objects

        // dump variant frequency map
        for( java.util.Map.Entry<String, Integer> entry: varTypeMap.entrySet() ) {
            System.out.println("  "+entry.getKey()+":  "+entry.getValue());
        }
    }

    String parseVarType( String info, java.util.Map<String,Integer> freqMap ) {

        // RS=1639538116;SSR=0;PSEUDOGENEINFO=DDX11L1:100287102;VC=SNV;R5;GNO;FREQ=dbGaP_PopFreq:1,0,0
        //
        // we look for ";VC=SNV;" and we take SNV as var type etc
        int pos1 = info.indexOf(";VC=");
        if( pos1 > 0 ) {
            int pos2 = info.indexOf(";", pos1 + 4);
            String varType = null;
            if( pos2 < 0 ) {
                varType = info.substring(pos1 + 4);
            } else if( pos2 > pos1 ) {
                varType = info.substring(pos1 + 4, pos2);
            }
            if( varType!=null ) {

                Integer count = freqMap.get(varType);
                if( count == null ) {
                    count = 1;
                } else {
                    count++;
                }
                freqMap.put( varType, count );

                return varType;
            }
        }
        return null;
    }

}
