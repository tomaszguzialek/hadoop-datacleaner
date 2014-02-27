/**
 * eobjects.org AnalyzerBeans
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.hadoopdatacleaner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.transform.ConcatenatorTransformer;
import org.eobjects.analyzer.beans.transform.TokenizerTransformer;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.eobjects.analyzer.job.builder.AnalyzerJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableReducer;
import org.eobjects.metamodel.pojo.ArrayTableDataProvider;
import org.eobjects.metamodel.pojo.TableDataProvider;
import org.eobjects.metamodel.util.SimpleTableDef;

public final class HBaseTool extends Configured implements Tool {

    public final static String ANALYSIS_JOB_XML_KEY = "analysis.job.xml";
    public final static String ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY = "analyzer.beans.configuration.datastores.key";
    
    private String analyzerBeansConfigurationDatastores;
    private String analysisJobXml;

    public HBaseTool(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob) {

        this.analyzerBeansConfigurationDatastores = ConfigurationSerializer
                .serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
        this.analysisJobXml = ConfigurationSerializer
                .serializeAnalysisJobToXml(analyzerBeansConfiguration, analysisJob);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputTableName, outputTableName;
        if (args.length == 2) {
            inputTableName = args[0];
            outputTableName = args[1];
        } else {
            System.err.println("Incorrect number of arguments.  Expected: <inputTableName> <outputTableName>");
            return -1;
        }

        Configuration mapReduceConfiguration = HBaseConfiguration.create();
        mapReduceConfiguration.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY, analyzerBeansConfigurationDatastores);
        mapReduceConfiguration.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);

        return runMapReduceJob(inputTableName, outputTableName, mapReduceConfiguration);
    }

    private int runMapReduceJob(String inputTableName, String outputTableName, Configuration mapReduceConfiguration)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(mapReduceConfiguration);
        job.setJarByClass(HBaseTableMapper.class);
        job.setJobName(this.getClass().getName());

        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for
                              // MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(inputTableName, // input HBase
                                                              // table name
                scan, // Scan instance to control CF and attribute selection
                HBaseTableMapper.class, // mapper
                ImmutableBytesWritable.class, // mapper output key
                Result.class, // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(outputTableName, // output HBase
                                                                // table name
                HBaseTableReducer.class, // reducer class
                job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        AnalyzerBeansConfiguration analyzerBeansConfiguration = buildAnalyzerBeansConfiguration();
        AnalysisJob analysisJob = buildAnalysisJob(analyzerBeansConfiguration);
        HBaseTool hBaseTool = new HBaseTool(analyzerBeansConfiguration, analysisJob);
        ToolRunner.run(hBaseTool, args);
    }
    
    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration() {
        List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
        SimpleTableDef tableDef1 = new SimpleTableDef("countrycodes", new String[] {"mainFamily:country_name", "mainFamily:iso2", "mainFamily:iso3"});
        SimpleTableDef tableDef2 = new SimpleTableDef("countrycodes_output", new String[] {"mainFamily:country_name", "mainFamily:iso2", "mainFamily:iso3"});
        tableDataProviders.add(new ArrayTableDataProvider(tableDef1, new ArrayList<Object[]>()));
        tableDataProviders.add(new ArrayTableDataProvider(tableDef2, new ArrayList<Object[]>()));
        Datastore datastore = new PojoDatastore("countrycodes_hbase", "countrycodes_schema", tableDataProviders);
        
        DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastore);

        SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider(
                true);
        descriptorProvider.addTransformerBeanDescriptor(Descriptors
                .ofTransformer(ConcatenatorTransformer.class));
        descriptorProvider.addTransformerBeanDescriptor(Descriptors
                .ofTransformer(TokenizerTransformer.class));
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
                .ofAnalyzer(InsertIntoTableAnalyzer.class));
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors
                .ofAnalyzer(StringAnalyzer.class));

        return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
                .replace(descriptorProvider);
    }

    public static AnalysisJob buildAnalysisJob(
            AnalyzerBeansConfiguration configuration) {
        AnalysisJobBuilder ajb = new AnalysisJobBuilder(configuration);
        try {
            ajb.setDatastore("countrycodes_hbase");
            
            ajb.addSourceColumns("countrycodes.mainFamily:country_name",
                    "countrycodes.mainFamily:iso2",
                    "countrycodes.mainFamily:iso3");

            TransformerJobBuilder<ConcatenatorTransformer> concatenator = ajb
                    .addTransformer(ConcatenatorTransformer.class);
            concatenator.addInputColumns(ajb.getSourceColumnByName("mainFamily:iso2"));
            concatenator.addInputColumns(ajb.getSourceColumnByName("mainFamily:iso3"));
            concatenator.setConfiguredProperty("Separator", "_");
            
//          TransformerJobBuilder<TokenizerTransformer> tokenizer = ajb.addTransformer(TokenizerTransformer.class);
//          tokenizer.setConfiguredProperty("Token target", TokenizerTransformer.TokenTarget.COLUMNS);
//          tokenizer.addInputColumns(concatenator.getOutputColumns().get(0));
//          tokenizer.setConfiguredProperty("Number of tokens", 2);
//          tokenizer.setConfiguredProperty("Delimiters", new char[] { '_' });
//          tokenizer.getOutputColumns().get(0).setName("tokenized");
            
            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer = ajb.addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer.addInputColumn(ajb.getSourceColumnByName("mainFamily:country_name"));

            return ajb.toAnalysisJob();
        } finally {
            ajb.close();
        }
    }

}
