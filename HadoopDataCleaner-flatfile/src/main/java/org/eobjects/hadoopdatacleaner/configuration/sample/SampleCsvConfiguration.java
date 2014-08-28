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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.UrlResource;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.CsvDatastore;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;

public class SampleCsvConfiguration {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration(String csvFilePath) {
        Resource resourceInput = new UrlResource(csvFilePath);
        
        CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8", ';', '"', '\\');
        Datastore datastoreInput = new CsvDatastore(csvFilePath.substring(csvFilePath.lastIndexOf('/') + 1),
                resourceInput, csvConfiguration);
        
        DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastoreInput);

//        ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
//        descriptorProvider.scanPackage("org.eobjects", true);
//        descriptorProvider.scanPackage("com.hi", true);
        
        List<String> transformers = new ArrayList<String>();
        transformers.add("com.hi.contacts.datacleaner.NameTransformer");
        transformers.add("com.hi.contacts.datacleaner.EmailTransformer");
        transformers.add("com.hi.contacts.datacleaner.AddressTransformer");
        transformers.add("com.hi.contacts.datacleaner.PhoneTransformer");
        
        List<String> analyzers = new ArrayList<String>();
        analyzers.add("org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer");
        
        SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider();
        try {
			descriptorProvider.setTransformerClassNames(transformers);
			descriptorProvider.setAnalyzerClassNames(analyzers);
			return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
					.replace(descriptorProvider);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
    }
    

}
