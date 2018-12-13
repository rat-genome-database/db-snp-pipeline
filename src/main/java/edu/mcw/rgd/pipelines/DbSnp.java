package edu.mcw.rgd.pipelines;


import edu.mcw.rgd.process.Utils;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * @author mtutaj
 * @since 9/27/11
 * represents a row from DB_SNP table
 */
public class DbSnp {

    private String snpName;
    private String source; // aka BUILD
    private int mapKey;
    private String chromosome;
    private int position;
    private String snpClass;
    private String snpType;
    private String molType;
    private String genotype;
    private Double avgHetroScore;
    private Double stdError;
    private String hetroType;
    private String allele;
    private Integer orientation;
    private Double mafFrequency;
    private Integer mafSampleSize;
    private String mafAllele;
    private String functionClass;
    private int mapLocCount;
    private String ancestralAllele;
    private String clinicalSignificance;
    private String refAllele;
    private List<String> hgvs;
    private Map<String, Integer> mergeHistory;

    @Override
    public boolean equals(Object obj) {

        DbSnp o = (DbSnp) obj;

        return
            Utils.stringsAreEqual(this.snpName, o.snpName) &&
            Utils.stringsAreEqual(this.source, o.source) &&
            this.mapKey==o.mapKey &&
            Utils.stringsAreEqual(this.chromosome, o.chromosome) &&
            this.position==o.position &&
            Utils.stringsAreEqual(this.snpClass, o.snpClass) &&
            Utils.stringsAreEqual(this.snpType, o.snpType) &&
            Utils.stringsAreEqual(this.molType, o.molType) &&
            Utils.stringsAreEqual(this.genotype, o.genotype) &&
            doublesAreEqual(this.avgHetroScore, o.avgHetroScore, 2)  &&
            doublesAreEqual(this.stdError, o.stdError, 4) &&
            Utils.stringsAreEqual(this.hetroType, o.hetroType) &&
            Utils.stringsAreEqual(this.allele, o.allele) &&
            Utils.intsAreEqual(this.orientation, o.orientation) &&
            doublesAreEqual(this.mafFrequency, o.mafFrequency, 4) &&
            Utils.intsAreEqual(this.mafSampleSize, o.mafSampleSize) &&
            Utils.stringsAreEqual(this.mafAllele, o.mafAllele) &&
            Utils.stringsAreEqual(this.functionClass, o.functionClass) &&
            this.mapLocCount==o.mapLocCount &&
            Utils.stringsAreEqual(this.ancestralAllele, o.ancestralAllele) &&
            Utils.stringsAreEqual(this.refAllele, o.refAllele) &&
            Utils.stringsAreEqual(this.clinicalSignificance, o.clinicalSignificance)
        ;
    }

    /**
     * return true if both double precision numbers are equal up to specific precision;
     * null numbers are treated as 0.0
     * @param d1 first double; if null, 0.0 is used in comparison
     * @param d2 second double; if null, 0.0 is used in comparison
     * @param prec precision: how many digits after the decimal point
     * @return true if both double precision numbers are equal up to given precision
     */
    public static boolean doublesAreEqual(Double d1, Double d2, int prec) {
        if( d1==null )
            d1 = 0.0;
        if( d2==null )
            d2 = 0.0;
        DecimalFormat df = new DecimalFormat();
        df.setMinimumFractionDigits(prec);
        df.setMaximumFractionDigits(prec);
        String s1 = df.format(d1);
        String s2 = df.format(d2);
        return s1.equals(s2);
    }

    public String getSnpName() {
        return snpName;
    }

    public void setSnpName(String snpName) {
        this.snpName = snpName;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public String getChromosome() {
        return chromosome;
    }

    public void setChromosome(String chromosome) {
        this.chromosome = chromosome;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getSnpClass() {
        return snpClass;
    }

    public void setSnpClass(String snpClass) {
        this.snpClass = snpClass;
    }

    public String getSnpType() {
        return snpType;
    }

    public void setSnpType(String snpType) {
        this.snpType = snpType;
    }

    public String getMolType() {
        return molType;
    }

    public void setMolType(String molType) {
        this.molType = molType;
    }

    public String getGenotype() {
        return genotype;
    }

    public void setGenotype(String genotype) {
        this.genotype = genotype;
    }

    public Double getAvgHetroScore() {
        return avgHetroScore;
    }

    public void setAvgHetroScore(Double avgHetroScore) {
        this.avgHetroScore = avgHetroScore;
    }

    public Double getStdError() {
        return stdError;
    }

    public void setStdError(Double stdError) {
        this.stdError = stdError;
    }

    public String getHetroType() {
        return hetroType;
    }

    public void setHetroType(String hetroType) {
        this.hetroType = hetroType;
    }

    public String getAllele() {
        return allele;
    }

    public void setAllele(String allele) {
        this.allele = allele;
    }

    public Integer getOrientation() {
        return orientation;
    }

    public void setOrientation(Integer orientation) {
        this.orientation = orientation;
    }

    public Double getMafFrequency() {
        return mafFrequency;
    }

    public void setMafFrequency(Double mafFrequency) {
        this.mafFrequency = mafFrequency;
    }

    public Integer getMafSampleSize() {
        return mafSampleSize;
    }

    public void setMafSampleSize(Integer mafSampleSize) {
        this.mafSampleSize = mafSampleSize;
    }

    public String getMafAllele() {
        return mafAllele;
    }

    public void setMafAllele(String mafAllele) {
        this.mafAllele = mafAllele;
    }

    public String getFunctionClass() {
        return functionClass;
    }

    public void setFunctionClass(String functionClass) {
        this.functionClass = functionClass;
    }

    public int getMapLocCount() {
        return mapLocCount;
    }

    public void setMapLocCount(int mapLocCount) {
        this.mapLocCount = mapLocCount;
    }

    public String getAncestralAllele() {
        return ancestralAllele;
    }

    public void setAncestralAllele(String ancestralAllele) {
        this.ancestralAllele = ancestralAllele;
    }

    public String getClinicalSignificance() {
        return clinicalSignificance;
    }

    public void setClinicalSignificance(String clinicalSignificance) {
        this.clinicalSignificance = clinicalSignificance;
    }

    public String getRefAllele() {
        return refAllele;
    }

    public void setRefAllele(String refAllele) {
        this.refAllele = refAllele;
    }

    public List<String> getHgvs() {
        return hgvs;
    }

    public void setHgvs(List<String> hgvs) {
        this.hgvs = hgvs;
    }

    public Map<String, Integer> getMergeHistory() {
        return mergeHistory;
    }

    public void setMergeHistory(Map<String, Integer> mergeHistory) {
        this.mergeHistory = mergeHistory;
    }
}

