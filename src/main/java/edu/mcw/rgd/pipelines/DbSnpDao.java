package edu.mcw.rgd.pipelines;

import edu.mcw.rgd.dao.AbstractDAO;
import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.MapDAO;
import edu.mcw.rgd.datamodel.Chromosome;
import edu.mcw.rgd.process.Utils;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.*;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author mtutaj
 * @since 9/27/11
 * all database access code lands here
 */
public class DbSnpDao extends AbstractDAO {

    private int batchSize;

    static long dbSnpCount = 0;
    static long hgvsNamesCount = 0;
    static long mergeHistoryCount = 0;
    static NumberFormat _fmtThousands = NumberFormat.getNumberInstance(Locale.US);

    static long committedDbSnpBatches = 0;
    static long committedDbSnpBatchesElapsedTime = 0;

    //
    public DataSource getDataSource() throws Exception {
        return DataSourceFactory.getInstance().getDataSource("DbSnp");
    }

    String getStats() {
        return "  dbsnp=" + _fmtThousands.format(dbSnpCount)
                + "  hgvs=" + _fmtThousands.format(hgvsNamesCount)
                + "  hist=" + _fmtThousands.format(mergeHistoryCount);
    }

    public void printStats() {
        System.out.println(getStats());
    }

    public void printStats(String chr) {
        System.out.println("chr"+chr+getStats());
    }

    /**
     * insert a batch of DbSnp objects into database
     * @param dbSnps List of DbSnp objects
     * @return nr of rows affected
     * @throws Exception
     */
    public int insert(List<DbSnp> dbSnps) throws Exception {

        String chr = "";
        if( !dbSnps.isEmpty() ) {
            chr = dbSnps.get(0).getChromosome()+" "+dbSnps.get(0).getSnpName()+" ";
        }

        long time0 = System.currentTimeMillis();
        int cnt0 = Math.abs(insertDbSnps(dbSnps))/2;
        dbSnpCount += cnt0;
        long time1 = System.currentTimeMillis();

        committedDbSnpBatches++;
        committedDbSnpBatchesElapsedTime += time1-time0;
        System.out.println("  INS DB_SNP "+ Utils.formatElapsedTime(time0, time1)
                +"   AVG="+Utils.formatElapsedTime(0, committedDbSnpBatchesElapsedTime/committedDbSnpBatches));

        int cnt1 = Math.abs(insertHgvsNames(dbSnps))/2;
        hgvsNamesCount += cnt1;

        int cnt2 = Math.abs(insertMergeHistory(dbSnps))/2;
        mergeHistoryCount += cnt2;

        printStats(chr);

        return cnt0+cnt1+cnt2;
    }

    /**
     * insert a batch of db snps into DB_SNP table
     * @param dbSnps List of DbSnp objects
     * @return nr of rows affected
     * @throws Exception
     */
    public int insertDbSnps(List<DbSnp> dbSnps) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(), """
            INSERT INTO DB_SNP (chromosome, position, avg_hetro_score, std_error,
            snp_name, source, map_key, allele,
            maf_frequency, maf_sample_size, het_type,
            snp_class, mol_type, genotype,
            map_loc_count, function_class, maf_allele,
            clinical_significance, ref_allele, db_snp_id)
            SELECT ?,?,?,?, ?,?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,DB_SNP_SEQ.NEXTVAL FROM dual
            WHERE NOT EXISTS(SELECT 1 FROM DB_SNP WHERE chromosome=? AND position=? AND snp_name=?
            AND source=? AND map_key=? AND allele=?)
           """,
            new int[] {Types.VARCHAR, Types.INTEGER, Types.DOUBLE, Types.DOUBLE,
                    Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                    Types.DOUBLE, Types.INTEGER, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                    Types.INTEGER, Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                    Types.VARCHAR, Types.INTEGER, Types.VARCHAR
            }
        );
        su.setBatchSize(getBatchSize());
        su.compile();

        for( DbSnp snp: dbSnps ) {
            su.update(snp.getChromosome(), snp.getPosition(), snp.getAvgHetroScore(), snp.getStdError(),
                    snp.getSnpName(), snp.getSource(), snp.getMapKey(), snp.getAllele(),
                    snp.getMafFrequency(), snp.getMafSampleSize(), snp.getHetroType(),
                    snp.getSnpClass(), snp.getMolType(), snp.getGenotype(),
                    snp.getMapLocCount(), snp.getFunctionClass(), snp.getMafAllele(),
                    snp.getClinicalSignificance(), snp.getRefAllele(),
                    snp.getChromosome(), snp.getPosition(), snp.getSnpName(),
                    snp.getSource(), snp.getMapKey(), snp.getAllele());
        }

        return executeBatch(su);
    }

    /**
     * insert a batch of HGVS names into DB_SNP_HGVS table
     * @param dbSnps List of DbSnp objects
     * @return nr of rows affected
     * @throws Exception
     */
    public int insertHgvsNames(List<DbSnp> dbSnps) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(),
            "INSERT INTO db_snp_hgvs (snp_name,hgvs_name) "+
            "SELECT ?,? FROM dual "+
            "WHERE NOT EXISTS (SELECT 1 FROM db_snp_hgvs WHERE snp_name=? AND hgvs_name=?)",
            new int[] {Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}
        );
        su.setBatchSize(getBatchSize());
        su.compile();

        for( DbSnp snp: dbSnps ) {
            if( snp.getHgvs()==null ) {
                continue;
            }
            for( String hgvsName: snp.getHgvs() ) {
                su.update(snp.getSnpName(), hgvsName, snp.getSnpName(), hgvsName);
            }
        }

        return executeBatch(su);
    }

    /**
     * insert a batch of HGVS names into DB_SNP_HGVS table
     * @param dbSnps List of DbSnp objects
     * @return nr of rows affected
     * @throws Exception
     */
    public int insertMergeHistory(List<DbSnp> dbSnps) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(),
            "INSERT INTO db_snp_merge_history (snp_name,old_snp_name,build_id) "+
            "SELECT ?,?,? FROM dual "+
            "WHERE NOT EXISTS (SELECT 1 FROM db_snp_merge_history WHERE snp_name=? AND old_snp_name=?)",
            new int[] {Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR}
        );
        su.setBatchSize(getBatchSize());
        su.compile();

        for( DbSnp snp: dbSnps ) {
            if( snp.getMergeHistory()==null ) {
                continue;
            }
            for( Map.Entry<String, Integer> entry: snp.getMergeHistory().entrySet() ) {
                String oldSnpName = entry.getKey();
                Integer buildId = entry.getValue();
                su.update(snp.getSnpName(), oldSnpName, buildId, snp.getSnpName(), oldSnpName);
            }
        }

        return executeBatch(su);
    }

    /**
     * update MAF_ALLELE for a batch of DbSnp objects
     * @param dbSnps List of DbSnp objects
     * @return nr of rows affected
     * @throws Exception
     */
    public int updateMafAllele(List<DbSnp> dbSnps) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(),
            "UPDATE DB_SNP SET maf_allele=? WHERE map_key=? AND source=? AND snp_name=? AND chromosome=? AND position=? ",
            new int[] {Types.VARCHAR, Types.INTEGER, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.INTEGER
            }
        );
        su.setBatchSize(getBatchSize());
        su.compile();

        for( DbSnp snp: dbSnps ) {
            if( snp.getMafAllele()!=null ) {
                su.update(new Object[]{snp.getMafAllele(), snp.getMapKey(), snp.getSource(),
                    snp.getSnpName(), snp.getChromosome(), snp.getPosition(),
                });
            }
        }

        return executeBatch(su);
    }

    public ResultSet getDataSet(String source, int mapKey) throws Exception {

        String sql = "SELECT chromosome, position, snp_name FROM DB_SNP WHERE source=? AND map_key=?";
        Connection conn = null;
        try {
            conn = this.getConnection();
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setString(1, source);
            pst.setInt(2, mapKey);
            return pst.executeQuery();
        }
        catch(Exception e) {
            if( conn!=null ) {
                try { conn.close(); } catch(Exception ignored) {}
            }
            throw e;
        }
    }

    public Set<DbSnp> getDbSnp(String source, int mapKey,String chromosome) throws Exception {

        String sql = "SELECT * FROM DB_SNP WHERE source=? AND map_key=? and chromosome = ? and ref_allele is null";
        Connection conn = null;
        try {
            conn = this.getConnection();
            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setString(1, source);
            pst.setInt(2, mapKey);
            pst.setString(3,chromosome);
            ResultSet rs =  pst.executeQuery();
            Set<DbSnp> result = new TreeSet<>(new Comparator() {
                @Override
                public int compare(Object o1, Object o2) {
                    if(((DbSnp)o1).getSnpName().equals(((DbSnp)o2).getSnpName())) {
                        if(((DbSnp)o1).getPosition() == (((DbSnp)o2).getPosition())){
                            return 0;
                        }else return 1;
                    } else return 1;

                }
            });
            while(rs.next()){
                DbSnp dbSnp = new DbSnp();
                dbSnp.setSnpName(rs.getString("SNP_NAME"));
                dbSnp.setSource(rs.getString("SOURCE"));
                dbSnp.setMapKey(rs.getInt("MAP_KEY"));
                dbSnp.setChromosome(rs.getString("CHROMOSOME"));
                dbSnp.setPosition(rs.getInt("POSITION"));
                result.add(dbSnp);
            }
            System.out.println("Size " + result.size());
            conn.close();
            return result;
        }
        catch(Exception e) {
            if( conn!=null ) {
                try { conn.close(); } catch(Exception ignored) {}
            }
            throw e;
        }
    }
    public int updateDbSnp(List<DbSnp> dbSnp,String source) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(this.getDataSource(),
                "UPDATE DB_SNP SET ref_allele = ? WHERE source=? AND map_key=? AND snp_name=? and chromosome = ? and position = ?",
                new int[] {Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.VARCHAR, Types.VARCHAR, Types.INTEGER
                }
        );
        su.setBatchSize(getBatchSize());
        su.compile();

        for( DbSnp snp: dbSnp ) {

            su.update(new Object[]{snp.getRefAllele(),source, snp.getMapKey(),
                    snp.getSnpName(), snp.getChromosome(),snp.getPosition()
            });

        }

        return executeBatch(su);

    }
    public void delete(String source, String chromosome, int mapKey) throws Exception {


        String sql = "Delete from DB_SNP where ref_allele = allele and map_key = ? and source = ? and chromosome = ?";

        Connection conn = null;
        try {
            conn = this.getDataSource().getConnection();
            conn.setAutoCommit(false);


            PreparedStatement pst = conn.prepareStatement(sql);
            pst.setInt(1, mapKey);
            pst.setString(2, source);
            pst.setString(3, chromosome);
            pst.executeUpdate();



            conn.commit();

            conn.setAutoCommit(true);
        }
        finally {
            if( conn!=null ) {
                try { conn.close(); } catch(Exception ignored) {}
            }
        }
    }
    public void upConvert(String source, int mapKey1, int mapKey2) throws Exception {

        String sql1 = "SELECT avg_hetro_score, std_error, allele, "+
                "orientation, maf_frequency, maf_sample_size, maf_allele, het_type, "+
                "snp_class, snp_type, mol_type, genotype, "+
                "map_loc_count, function_class, snp_name FROM DB_SNP WHERE source=? AND map_key=?";

        String sql2 = "UPDATE DB_SNP SET avg_hetro_score=?, std_error=?, allele=?, "+
                "orientation=?, maf_frequency=?, maf_sample_size=?, maf_allele=?, het_type=?, "+
                "snp_class=?, snp_type=?, mol_type=?, genotype=?, "+
                "map_loc_count=?, function_class=? WHERE source=? AND map_key=? AND snp_name=?";

        Connection conn = null;
        try {
            conn = this.getConnection();
            conn.setAutoCommit(false);
            int rowsProcessed = 0;

            PreparedStatement pst = conn.prepareStatement(sql1);
            pst.setString(1, source);
            pst.setInt(2, mapKey1);

            PreparedStatement pst2 = conn.prepareStatement(sql2);

            ResultSet rs = pst.executeQuery();
            while( rs.next() ) {

                pst2.setString(1, rs.getString(1)); // avg_hetro_score
                pst2.setString(2, rs.getString(2)); // std_error
                pst2.setString(3, rs.getString(3)); // allele
                pst2.setString(4, rs.getString(4)); // orient
                pst2.setString(5, rs.getString(5)); // maf_freq
                pst2.setString(6, rs.getString(6)); // maf_sample_size
                pst2.setString(7, rs.getString(7)); // maf_allele
                pst2.setString(8, rs.getString(8)); // het_type
                pst2.setString(9, rs.getString(9)); // snp_class
                pst2.setString(10, rs.getString(10)); // snp_type
                pst2.setString(11, rs.getString(11)); // mol_type
                pst2.setString(12, rs.getString(12)); // genotype
                pst2.setString(13, rs.getString(13)); // map_loc_count
                pst2.setString(14, rs.getString(14)); // function_class

                pst2.setString(15, source);
                pst2.setInt(16, mapKey2);
                pst2.setString(17, rs.getString(15));

                pst2.executeUpdate();

                if( ++rowsProcessed%12345==0 ) {
                    conn.commit();
                    System.out.println("committed "+rowsProcessed);
                }
            }

            conn.commit();
            System.out.println("committed "+rowsProcessed);

            conn.setAutoCommit(true);
        }
        finally {
            if( conn!=null ) {
                try { conn.close(); } catch(Exception ignored) {}
            }
        }
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public java.util.Map<String,Integer> getChromosomeSizes(int mapKey) throws Exception {
        MapDAO dao = new MapDAO();
        return dao.getChromosomeSizes(mapKey);
    }

    public java.util.Map<String,String> getRefSeqToChrMap(int mapKey) throws Exception {
        MapDAO dao = new MapDAO();
        java.util.Map<String,String> map = new HashMap<>();
        List<Chromosome> chromosomes = dao.getChromosomes(mapKey);
        for( Chromosome c: chromosomes ) {
            map.put(c.getRefseqId(), c.getChromosome());
        }
        return map;
    }

}
