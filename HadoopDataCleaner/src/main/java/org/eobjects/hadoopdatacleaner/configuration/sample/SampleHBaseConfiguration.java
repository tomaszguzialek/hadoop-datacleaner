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

import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.filter.EqualsFilter;
import org.eobjects.analyzer.beans.filter.ValidationCategory;
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
import org.eobjects.analyzer.job.builder.FilterJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
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

        SimpleDescriptorProvider descriptorProvider = new SimpleDescriptorProvider(true);
        descriptorProvider.addTransformerBeanDescriptor(Descriptors.ofTransformer(ConcatenatorTransformer.class));
        descriptorProvider.addTransformerBeanDescriptor(Descriptors.ofTransformer(TokenizerTransformer.class));
        descriptorProvider.addFilterBeanDescriptor(Descriptors.ofFilter(EqualsFilter.class));
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors.ofAnalyzer(InsertIntoTableAnalyzer.class));
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors.ofAnalyzer(StringAnalyzer.class));

        return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog).replace(descriptorProvider);
    }

    public static AnalysisJob buildAnalysisJob(AnalyzerBeansConfiguration configuration) {
        AnalysisJobBuilder ajb = new AnalysisJobBuilder(configuration);
        try {
            ajb.setDatastore("countrycodes_hbase");

            ajb.addSourceColumns("countrycodes_schema.countrycodes.mainFamily:country_name",
                    "countrycodes_schema.countrycodes.mainFamily:iso2",
                    "countrycodes_schema.countrycodes.mainFamily:iso3");

            TransformerJobBuilder<ConcatenatorTransformer> concatenator = ajb
                    .addTransformer(ConcatenatorTransformer.class);
            concatenator.addInputColumns(ajb.getSourceColumnByName("mainFamily:iso2"));
            concatenator.addInputColumns(ajb.getSourceColumnByName("mainFamily:iso3"));
            concatenator.setConfiguredProperty("Separator", "_");
            concatenator.getOutputColumns().get(0).setName("mainFamily:iso2_iso3");
            
            FilterJobBuilder<EqualsFilter, ?> equalsFilter = ajb.addFilter(EqualsFilter.class);
            equalsFilter.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso2"));
            equalsFilter.setConfiguredProperty("Column", ajb.getSourceColumnByName("mainFamily:iso2"));
            equalsFilter.setConfiguredProperty("Values", new String[] {"PL", "DK"});

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
            valueDistributionAnalyzer.setRequirement(equalsFilter.getOutcome(ValidationCategory.INVALID));
            valueDistributionAnalyzer.addInputColumn(ajb.getSourceColumnByName("mainFamily:country_name"));

            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer2 = ajb
                    .addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer2.setRequirement(equalsFilter.getOutcome(ValidationCategory.VALID));
            valueDistributionAnalyzer2.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso2"));

            // AnalyzerJobBuilder<ValueDistributionAnalyzer>
            // valueDistributionAnalyzer3 =
            // ajb.addAnalyzer(ValueDistributionAnalyzer.class);
            // valueDistributionAnalyzer3.addInputColumn(ajb.getSourceColumnByName("mainFamily:country_name"));
            // valueDistributionAnalyzer3.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso2"));

            AnalyzerJobBuilder<StringAnalyzer> stringAnalyzer = ajb.addAnalyzer(StringAnalyzer.class);
            stringAnalyzer.setRequirement(equalsFilter.getOutcome(ValidationCategory.VALID));
            stringAnalyzer.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso3"));

            AnalyzerJobBuilder<InsertIntoTableAnalyzer> insertInto = ajb.addAnalyzer(InsertIntoTableAnalyzer.class);
            insertInto.setConfiguredProperty("Column names", new String[] { "mainFamily:iso2" });
            insertInto.setConfiguredProperty("Table name", "countrycodes_output");
            insertInto.setConfiguredProperty("Schema name", "countrycodes_schema");
            insertInto.setConfiguredProperty("Datastore",
                    configuration.getDatastoreCatalog().getDatastore("countrycodes_hbase"));
            insertInto.setRequirement(equalsFilter.getOutcome(ValidationCategory.INVALID));
            insertInto.addInputColumn(concatenator.getOutputColumns().get(0));

            return ajb.toAnalysisJob();
        } finally {
            ajb.close();
        }
    }
}
