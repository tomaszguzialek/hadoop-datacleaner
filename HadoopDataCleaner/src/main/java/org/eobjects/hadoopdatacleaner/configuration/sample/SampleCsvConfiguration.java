/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference

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
package org.eobjects.hadoopdatacleaner.configuration.sample;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
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
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.util.FileResource;
import org.eobjects.metamodel.util.UrlResource;

public class SampleCsvConfiguration {

    private static String _csvFilePath;

    static {
        // java.lang.StackOverflow workaround
        // http://stackoverflow.com/questions/17360018/getting-stack-overflow-error-in-hadoop
        Configuration conf = new Configuration();
        try {
            FileSystem.getFileSystemClass("file", conf);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        // The end of the workaround
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfigurationHdfs(String csvFilePath) {
        _csvFilePath = csvFilePath;
        
        CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8", ';', '"', '\\');
        Datastore datastore = new CsvDatastore(csvFilePath,
                new UrlResource(csvFilePath), csvConfiguration);
        
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
    
    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfigurationLocal(String csvFilePath) {
        _csvFilePath = csvFilePath;
        
        CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8", ';', '"', '\\');
        Datastore datastore = new CsvDatastore(csvFilePath,
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

    public static AnalysisJob buildAnalysisJob(AnalyzerBeansConfiguration configuration) {
        AnalysisJobBuilder ajb = new AnalysisJobBuilder(configuration);
        try {
            ajb.setDatastore(_csvFilePath);
            ajb.addSourceColumns("countrycodes.csv.countrycodes.Country name",
                    "countrycodes.csv.countrycodes.ISO 3166-2", "countrycodes.csv.countrycodes.ISO 3166-3",
                    "countrycodes.csv.countrycodes.Synonym3");

            TransformerJobBuilder<ConcatenatorTransformer> concatenator = ajb
                    .addTransformer(ConcatenatorTransformer.class);
            concatenator.addInputColumns(ajb.getSourceColumnByName("countrycodes.csv.countrycodes.ISO 3166-2"));
            concatenator.addInputColumns(ajb.getSourceColumnByName("countrycodes.csv.countrycodes.ISO 3166-3"));
            concatenator.setConfiguredProperty("Separator", "_");

            // TransformerJobBuilder<TokenizerTransformer> tokenizer =
            // ajb.addTransformer(TokenizerTransformer.class);
            // tokenizer.setConfiguredProperty("Token target",
            // TokenizerTransformer.TokenTarget.COLUMNS);
            // tokenizer.addInputColumns(concatenator.getOutputColumns().get(0));
            // tokenizer.setConfiguredProperty("Number of tokens", 2);
            // tokenizer.setConfiguredProperty("Delimiters", new char[] { '_'
            // });
            // tokenizer.getOutputColumns().get(0).setName("tokenized");

            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer = ajb
                    .addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer.addInputColumn(ajb
                    .getSourceColumnByName("countrycodes.csv.countrycodes.Country name"));
            
            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer2 = ajb
                    .addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer2.addInputColumn(ajb
                    .getSourceColumnByName("countrycodes.csv.countrycodes.ISO 3166-2"));

            return ajb.toAnalysisJob();
        } finally {
            ajb.close();
        }
    }

}
