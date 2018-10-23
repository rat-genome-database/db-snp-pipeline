package edu.mcw.rgd.pipelines;

import org.springframework.jdbc.object.MappingSqlQuery;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA. <br>
 * User: mtutaj <br>
 * Date: 9/27/11 <br>
 * Time: 11:57 AM <br>
 * To change this template use File | Settings | File Templates.
 * <p/>
 * helper class to facilitate sql queries against DB_SNP table
 */
public class DbSnpQuery extends MappingSqlQuery {

    public DbSnpQuery(DataSource ds, String query) {
        super(ds, query);
    }

    protected Object mapRow(ResultSet rs, int rowNum) throws SQLException {


        DbSnp rec = new DbSnp();

        rec.setAllele(rs.getString("allele"));
        rec.setAvgHetroScore(rs.getDouble("avg_hetro_score"));
        rec.setChromosome(rs.getString("chromosome"));
        rec.setMafFrequency(rs.getDouble("maf_frequency"));
        rec.setMafSampleSize(rs.getInt("maf_sample_size"));
        rec.setMapKey(rs.getInt("map_key"));
        rec.setOrientation(rs.getInt("orientation"));
        rec.setPosition(rs.getInt("position"));
        rec.setSnpName(rs.getString("snp_name"));
        rec.setSource(rs.getString("source"));
        rec.setStdError(rs.getDouble("std_error"));
        return rec;
    }

}
