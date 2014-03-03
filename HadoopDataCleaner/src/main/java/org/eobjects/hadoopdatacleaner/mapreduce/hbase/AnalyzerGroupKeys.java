package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public enum AnalyzerGroupKeys {

    STRING_ANALYZER ("org.eobjects.analyzer.beans.StringAnalyzer"),
    VALUE_DISTRIBUTION_ANALYZER ("org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer");
    
    private String qualifiedClassName;
    
    private AnalyzerGroupKeys(String qualifiedClassName) {
        this.qualifiedClassName = qualifiedClassName;
    }
    
    public String value() {
        return qualifiedClassName;
    }
    
    public ImmutableBytesWritable getWritableKey() {
        return new ImmutableBytesWritable(Bytes.toBytes(qualifiedClassName));
    }
    
}
