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

import java.util.ArrayList;
import java.util.List;

import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.ClasspathScanDescriptorProvider;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;

public class SampleHBaseConfiguration {

    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration() {
        List<TableDataProvider<?>> inputTableDataProviders = new ArrayList<TableDataProvider<?>>();
        List<TableDataProvider<?>> outputTableDataProviders = new ArrayList<TableDataProvider<?>>();
        SimpleTableDef tableDef1 = new SimpleTableDef("input", new String[] { "mainFamily:id", "mainFamily:given_name",
                "mainFamily:family_name", "mainFamily:full_address", "mainFamily:email", "mainFamily:phone" });
        SimpleTableDef tableDef2 = new SimpleTableDef("output", new String[] { "mainFamily:id", "mainFamily:given_name",
                "mainFamily:family_name", "mainFamily:full_address", "mainFamily:email", "mainFamily:phone" });
        inputTableDataProviders.add(new ArrayTableDataProvider(tableDef1, new ArrayList<Object[]>()));
        outputTableDataProviders.add(new ArrayTableDataProvider(tableDef2, new ArrayList<Object[]>()));
        Datastore inputDatastore = new PojoDatastore("input_datastore", "input_schema", inputTableDataProviders);
        Datastore outputDatastore = new PojoDatastore("output_datastore", "output_schema", outputTableDataProviders);

        DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(inputDatastore, outputDatastore);

        List<String> transformers = new ArrayList<String>();
		transformers.add("org.eobjects.analyzer.beans.transform.ConcatenatorTransformer");
        transformers.add("com.hi.contacts.datacleaner.NameTransformer");
        transformers.add("com.hi.contacts.datacleaner.EmailTransformer");
        transformers.add("com.hi.contacts.datacleaner.AddressTransformer");
        transformers.add("com.hi.contacts.datacleaner.PhoneTransformer");
        transformers.add("org.eobjects.analyzer.beans.ParseJsonTransformer");
        transformers.add("org.eobjects.analyzer.beans.ComposeJsonTransformer");
        
        List<String> filters = new ArrayList<String>();
        filters.add("org.eobjects.analyzer.beans.filter.EqualsFilter");
        filters.add("org.eobjects.analyzer.beans.filter.StringValueRangeFilter");
        
        List<String> analyzers = new ArrayList<String>();
        analyzers.add("org.eobjects.analyzer.beans.writers.InsertIntoTableAnalyzer");
        
        SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider();
        try {
			descriptorProvider.setTransformerClassNames(transformers);
			descriptorProvider.setFilterClassNames(filters);
			descriptorProvider.setAnalyzerClassNames(analyzers);
			return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
					.replace(descriptorProvider);
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
    }

}
