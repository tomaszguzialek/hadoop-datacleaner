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
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;

public class SampleHBaseConfiguration {

    public static AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration() {
        List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
        SimpleTableDef tableDef1 = new SimpleTableDef("countrycodes", new String[] { "mainFamily:country_name",
                "mainFamily:iso2", "mainFamily:iso3" });
        SimpleTableDef tableDef2 = new SimpleTableDef("countrycodes_output", new String[] { "mainFamily:country_name",
                "mainFamily:iso2", "mainFamily:iso3" });
        tableDataProviders.add(new ArrayTableDataProvider(tableDef1, new ArrayList<Object[]>()));
        tableDataProviders.add(new ArrayTableDataProvider(tableDef2, new ArrayList<Object[]>()));
        Datastore datastore = new PojoDatastore("countrycodes_hbase", "countrycodes_schema", tableDataProviders);

        DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastore);

        ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
        descriptorProvider.scanPackage("org.eobjects", true);

        return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog).replace(descriptorProvider);
    }

}
