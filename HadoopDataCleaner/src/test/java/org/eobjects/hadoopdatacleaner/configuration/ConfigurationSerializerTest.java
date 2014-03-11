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
package org.eobjects.hadoopdatacleaner.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.eobjects.analyzer.beans.StringAnalyzer;
import org.eobjects.analyzer.beans.api.Analyzer;
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
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.MockInputRow;
import org.eobjects.analyzer.descriptors.Descriptors;
import org.eobjects.analyzer.descriptors.SimpleDescriptorProvider;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.builder.AnalysisJobBuilder;
import org.eobjects.analyzer.job.builder.AnalyzerJobBuilder;
import org.eobjects.analyzer.job.builder.TransformerJobBuilder;
import org.eobjects.analyzer.util.SchemaNavigator;
import org.eobjects.metamodel.pojo.ArrayTableDataProvider;
import org.eobjects.metamodel.pojo.TableDataProvider;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.SimpleTableDef;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationSerializerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationSerializer.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;
    private String analysisJobXml;

    @Before
    public void setUp() {
        this.analyzerBeansConfiguration = buildAnalyzerBeansConfiguration();
        this.analysisJobXml = hardcodedAnalysisJobXml();
    }

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
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors.ofAnalyzer(InsertIntoTableAnalyzer.class));
        descriptorProvider.addAnalyzerBeanDescriptor(Descriptors.ofAnalyzer(StringAnalyzer.class));

        return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog).replace(descriptorProvider);
    }

    @Test
    public void testSerializeDeserializeDatastores() {
        String csv = ConfigurationSerializer.serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
        logger.info("Csv: " + csv);
        AnalyzerBeansConfiguration deserialized = ConfigurationSerializer.deserializeAnalyzerBeansDatastores(csv);
        for (String datastoreName : analyzerBeansConfiguration.getDatastoreCatalog().getDatastoreNames()) {
            logger.info("Datastore: " + datastoreName);
            Datastore datastore = analyzerBeansConfiguration.getDatastoreCatalog().getDatastore(datastoreName);
            Datastore deserializedDatastore = deserialized.getDatastoreCatalog().getDatastore(datastoreName);
            Assert.assertNotNull(deserializedDatastore);

            SchemaNavigator schemaNavigator = datastore.openConnection().getSchemaNavigator();
            SchemaNavigator deserializedSchemaNavigator = deserializedDatastore.openConnection().getSchemaNavigator();
            for (Schema schema : schemaNavigator.getSchemas()) {
                String schemaName = schema.getName();
                logger.info("\tSchema: " + schemaName);
                Schema deserializedSchema = deserializedSchemaNavigator.getSchemaByName(schemaName);
                Assert.assertNotNull(deserializedSchema);

                for (Table table : schema.getTables()) {
                    String tableName = table.getName();
                    logger.info("\t\tTable: " + tableName);
                    Table deserializedTable = deserializedSchema.getTableByName(tableName);
                    Assert.assertNotNull(deserializedTable);

                    for (Column column : table.getColumns()) {
                        String columnName = column.getName();
                        logger.info("\t\t\tColumn: " + columnName);
                        Column deserializedColumn = deserializedTable.getColumnByName(columnName);
                        Assert.assertNotNull(deserializedColumn);
                    }
                }
            }
        }
    }

    @Test
    public void testDeserializeSerializeAnalysisJob() {
        AnalysisJob deserializedAnalysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml,
                analyzerBeansConfiguration);
        String serializedAnalysisJobXml = ConfigurationSerializer.serializeAnalysisJobToXml(analyzerBeansConfiguration,
                deserializedAnalysisJob);
        String expected = analysisJobXml.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ').replace('\f', ' ');
        String updatedTagStart = "<updated-date>";
        String updatedTagEnd = "</updated-date>";
        expected = expected.substring(0, expected.indexOf(updatedTagStart))
                + expected.substring(expected.indexOf(updatedTagEnd) + updatedTagEnd.length());
        serializedAnalysisJobXml = serializedAnalysisJobXml.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                .replace('\f', ' ');
        serializedAnalysisJobXml = serializedAnalysisJobXml.substring(0,
                serializedAnalysisJobXml.indexOf(updatedTagStart))
                + serializedAnalysisJobXml.substring(serializedAnalysisJobXml.indexOf(updatedTagEnd)
                        + updatedTagEnd.length());
        Assert.assertEquals(expected, serializedAnalysisJobXml);
    }

    @Test
    public void testInitializeStringAnalyzer() {
        AnalysisJob analysisJob = buildAnalysisJobForInitializeAnalyzerTest(analyzerBeansConfiguration);

        InputColumn<?> chosenColumn = null;
        Collection<InputColumn<?>> sourceColumns = analysisJob.getSourceColumns();
        for (InputColumn<?> inputColumn : sourceColumns) {
            if (inputColumn.getName().equals("mainFamily:iso3")) {
                chosenColumn = inputColumn;
                break;
            }
        }

        MockInputRow row = new MockInputRow();
        row.put(chosenColumn, "POL");

        Analyzer<?> analyzer = ConfigurationSerializer.initializeAnalyzer("String analyzer (mainFamily:iso3)",
                analyzerBeansConfiguration, analysisJob);
        analyzer.run(row, 1);

        logger.info(analyzer.getResult().toString());

    }

    @Test
    public void testInitializeValueDistributionAnalyzer() {
        AnalysisJob analysisJob = buildAnalysisJobForInitializeAnalyzerTest(analyzerBeansConfiguration);

        InputColumn<?> chosenColumn = null;
        Collection<InputColumn<?>> sourceColumns = analysisJob.getSourceColumns();
        for (InputColumn<?> inputColumn : sourceColumns) {
            if (inputColumn.getName().equals("mainFamily:iso2")) {
                chosenColumn = inputColumn;
                break;
            }
        }

        MockInputRow row = new MockInputRow();
        row.put(chosenColumn, "PL");

        Analyzer<?> analyzer = ConfigurationSerializer.initializeAnalyzer("Value distribution (mainFamily:iso2)",
                analyzerBeansConfiguration, analysisJob);
        analyzer.run(row, 1);

        logger.info(analyzer.getResult().toString());
    }

    private String hardcodedAnalysisJobXml() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?> <job xmlns=\"http://eobjects.org/analyzerbeans/job/1.0\">     <job-metadata>         <updated-date>2014-03-05-08:00</updated-date>     </job-metadata>     <source>         <data-context ref=\"countrycodes_hbase\"/>         <columns>             <column id=\"col_0\" path=\"countrycodes_schema.countrycodes.mainFamily:country_name\" type=\"VARCHAR\"/>             <column id=\"col_1\" path=\"countrycodes_schema.countrycodes.mainFamily:iso2\" type=\"VARCHAR\"/>             <column id=\"col_2\" path=\"countrycodes_schema.countrycodes.mainFamily:iso3\" type=\"VARCHAR\"/>         </columns>     </source>     <transformation>         <transformer>             <descriptor ref=\"Concatenator\"/>             <properties>                 <property name=\"Separator\" value=\"&lt;null&gt;\"/>             </properties>             <input ref=\"col_1\"/>             <input value=\"_\"/>             <input ref=\"col_2\"/>             <output name=\"concatenated\" id=\"col_3\"/>         </transformer>         <transformer>             <descriptor ref=\"Tokenizer\"/>             <properties>                 <property name=\"Delimiters\" value=\"[ ,_]\"/>                 <property name=\"Number of tokens\" value=\"2\"/>                 <property name=\"Token target\" value=\"COLUMNS\"/>             </properties>             <input ref=\"col_3\"/>             <output name=\"concatenated (token 1)\" id=\"col_4\"/>             <output name=\"concatenated (token 2)\" id=\"col_5\"/>         </transformer>     </transformation>     <analysis>         <analyzer>             <descriptor ref=\"String analyzer\"/>             <properties/>             <input ref=\"col_2\"/>             <input ref=\"col_0\"/>             <input ref=\"col_1\"/>             <input ref=\"col_3\"/>         </analyzer>     </analysis> </job> ";
    }

    public static AnalysisJob buildAnalysisJobForInitializeAnalyzerTest(AnalyzerBeansConfiguration configuration) {
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

            AnalyzerJobBuilder<ValueDistributionAnalyzer> valueDistributionAnalyzer = ajb
                    .addAnalyzer(ValueDistributionAnalyzer.class);
            valueDistributionAnalyzer.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso2"));

            AnalyzerJobBuilder<StringAnalyzer> stringAnalyzer = ajb.addAnalyzer(StringAnalyzer.class);
            stringAnalyzer.addInputColumn(ajb.getSourceColumnByName("mainFamily:iso3"));

            return ajb.toAnalysisJob();
        } finally {
            ajb.close();
        }
    }
}
