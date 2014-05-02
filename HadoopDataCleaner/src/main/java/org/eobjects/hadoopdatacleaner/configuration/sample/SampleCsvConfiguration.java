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
import org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.CsvDatastore;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.util.FileResource;
import org.eobjects.metamodel.util.Resource;
import org.eobjects.metamodel.util.UrlResource;

public class SampleCsvConfiguration {

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

    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration(String csvFilePath) {
        Resource resource;
        if (csvFilePath.startsWith("hdfs://")) {
            resource = new UrlResource(csvFilePath);
        } else {
            resource = new FileResource(csvFilePath);
        }
        
        CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8", ';', '"', '\\');
        Datastore datastore = new CsvDatastore(csvFilePath.substring(csvFilePath.lastIndexOf('/') + 1),
                resource, csvConfiguration);
        
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
    

}
