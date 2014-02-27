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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.transform.ConcatenatorTransformer;
import org.eobjects.analyzer.beans.transform.TokenizerTransformer;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.CsvDatastore;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.eobjects.analyzer.job.builder.AnalyzerJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileReducer;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.util.FileResource;

public final class FlatFileTool extends Configured implements Tool {

	public final static String ANALYSIS_JOB_XML_KEY = "analysis.job.xml";
	public final static String ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY = "analyzer.beans.configuration.datastores.key";

	private String analysisJobXml;

	private String analyzerBeansConfigurationDatastores;

	public FlatFileTool(
			AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) throws FileNotFoundException {

		this.analyzerBeansConfigurationDatastores = ConfigurationSerializer.serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
		this.analysisJobXml = ConfigurationSerializer.serializeAnalysisJobToXml(analyzerBeansConfiguration, analysisJob);
	}

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err
					.println("Incorrect number of arguments.  Expected: <input> output");
			return -1;
		}

		Configuration conf = getConf();
		conf.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);
		conf.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY,
				analyzerBeansConfigurationDatastores);

		return runMapReduceJob(input, output, conf);
	}

	private int runMapReduceJob(String analysisJobPath, String output,
			Configuration mapReduceConfiguration) throws IOException,
			InterruptedException, ClassNotFoundException {
	    
		Job job = Job.getInstance(mapReduceConfiguration);
		job.setJarByClass(FlatFileMapper.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(analysisJobPath));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(FlatFileMapper.class);
		job.setReducerClass(FlatFileReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		
		job.setNumReduceTasks(2);
		
		FileSystem hdfs = FileSystem.get(mapReduceConfiguration);
		if (hdfs.exists(new Path(output)))
			hdfs.delete(new Path(output), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        AnalyzerBeansConfiguration analyzerBeansConfiguration = buildCountryCodesAnalyzerBeansConfiguration(args[0]);
        AnalysisJob analysisJob = buildAnalysisJob(analyzerBeansConfiguration);
        FlatFileTool hadoopDataCleanerTool = new FlatFileTool(analyzerBeansConfiguration, analysisJob);
        ToolRunner.run(hadoopDataCleanerTool, args);
    }
	
	public static AnalyzerBeansConfiguration buildCountryCodesAnalyzerBeansConfiguration(String csvFilePath) {
        CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8", ';', '"', '\\');
        Datastore datastore = new CsvDatastore("Country codes",
                new FileResource(csvFilePath), csvConfiguration);
        
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
            ajb.setDatastore("Country codes");
            ajb.addSourceColumns("countrycodes.csv.countrycodes.Country name",
                    "countrycodes.csv.countrycodes.ISO 3166-2",
                    "countrycodes.csv.countrycodes.ISO 3166-3",
                    "countrycodes.csv.countrycodes.Synonym3");

            TransformerJobBuilder<ConcatenatorTransformer> concatenator = ajb
                    .addTransformer(ConcatenatorTransformer.class);
            concatenator.addInputColumns(ajb.getSourceColumnByName("countrycodes.csv.countrycodes.ISO 3166-2"));
            concatenator.addInputColumns(ajb.getSourceColumnByName("countrycodes.csv.countrycodes.ISO 3166-3"));
            concatenator.setConfiguredProperty("Separator", "_");
            
//          TransformerJobBuilder<TokenizerTransformer> tokenizer = ajb.addTransformer(TokenizerTransformer.class);
//          tokenizer.setConfiguredProperty("Token target", TokenizerTransformer.TokenTarget.COLUMNS);
//          tokenizer.addInputColumns(concatenator.getOutputColumns().get(0));
//          tokenizer.setConfiguredProperty("Number of tokens", 2);
//          tokenizer.setConfiguredProperty("Delimiters", new char[] { '_' });
//          tokenizer.getOutputColumns().get(0).setName("tokenized");
            
            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer = ajb.addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer.addInputColumn(ajb.getSourceColumnByName("countrycodes.csv.countrycodes.Country name"));

            return ajb.toAnalysisJob();
        } finally {
            ajb.close();
        }
    }

}
