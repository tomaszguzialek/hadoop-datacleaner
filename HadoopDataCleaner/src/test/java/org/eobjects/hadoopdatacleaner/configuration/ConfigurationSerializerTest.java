package org.eobjects.hadoopdatacleaner.configuration;

import junit.framework.Assert;

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
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.util.SchemaNavigator;
import org.eobjects.hadoopdatacleaner.configuration.sample.SampleHBaseConfiguration;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileResource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationSerializerTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationSerializer.class);
    
	private AnalyzerBeansConfiguration analyzerBeansConfigurationCsv;
	private AnalyzerBeansConfiguration analyzerBeansConfigurationHBase;
	private String analysisJobXml;

	@Before
	public void setUp() {
		this.analyzerBeansConfigurationCsv = buildAnalyzerBeansConfigurationCsv();
		this.analyzerBeansConfigurationHBase = SampleHBaseConfiguration.buildAnalyzerBeansConfiguration();

		this.analysisJobXml = hardcodedAnalysisJobXml();
	}

	private AnalyzerBeansConfiguration buildAnalyzerBeansConfigurationCsv() {
		CsvConfiguration csvConfiguration = new CsvConfiguration(1, "UTF8",
				';', '"', '\\');
		Datastore datastore = new CsvDatastore(
				"Country codes",
				new FileResource(
						"/home/cloudera/datacleaner_examples/countrycodes.csv"),
				csvConfiguration);

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

	@Test
	public void testSerializeDeserializeDatastoresFlatFile() {
		String csv = ConfigurationSerializer
				.serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfigurationCsv);
		logger.info("Csv: " + csv);
		AnalyzerBeansConfiguration deserialized = ConfigurationSerializer
				.deserializeAnalyzerBeansDatastores(csv);
		for (String datastoreName : analyzerBeansConfigurationCsv
				.getDatastoreCatalog().getDatastoreNames()) {
		    logger.info("Datastore: " + datastoreName);
			Datastore datastore = analyzerBeansConfigurationCsv
					.getDatastoreCatalog().getDatastore(datastoreName);
			Datastore deserializedDatastore = deserialized
					.getDatastoreCatalog().getDatastore(datastoreName);
			Assert.assertNotNull(deserializedDatastore);

			SchemaNavigator schemaNavigator = datastore.openConnection()
					.getSchemaNavigator();
			SchemaNavigator deserializedSchemaNavigator = deserializedDatastore
					.openConnection().getSchemaNavigator();
			for (Schema schema : schemaNavigator.getSchemas()) {
			    logger.info("\tSchema: " + schema.getName());
				Schema deserializedSchema = deserializedSchemaNavigator
						.getSchemaByName(schema.getName());
				Assert.assertNotNull(deserializedSchema);

				for (Table table : schema.getTables()) {
				    logger.info("\t\tTable: " + table.getName());
				    Table deserializedTable = deserializedSchema
							.getTableByName(table.getName());
					Assert.assertNotNull(deserializedTable);

					for (Column column : table.getColumns()) {
					    logger.info("\t\t\tColumn: " + column.getName());
					    Column deserializedColumn = deserializedTable
								.getColumnByName(column.getName());
						Assert.assertNotNull(deserializedColumn);
					}
				}
			}
		}
	}
	
	@Test
    public void testSerializeDeserializeDatastoresHBase() {
        String csv = ConfigurationSerializer
                .serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfigurationHBase);
        logger.info("Csv: " + csv);
        AnalyzerBeansConfiguration deserialized = ConfigurationSerializer
                .deserializeAnalyzerBeansDatastores(csv);
        for (String datastoreName : analyzerBeansConfigurationHBase
                .getDatastoreCatalog().getDatastoreNames()) {
            logger.info("Datastore: " + datastoreName);
            Datastore datastore = analyzerBeansConfigurationHBase
                    .getDatastoreCatalog().getDatastore(datastoreName);
            Datastore deserializedDatastore = deserialized
                    .getDatastoreCatalog().getDatastore(datastoreName);
            Assert.assertNotNull(deserializedDatastore);

            SchemaNavigator schemaNavigator = datastore.openConnection()
                    .getSchemaNavigator();
            SchemaNavigator deserializedSchemaNavigator = deserializedDatastore
                    .openConnection().getSchemaNavigator();
            for (Schema schema : schemaNavigator.getSchemas()) {
                String schemaName = schema.getName();
                logger.info("\tSchema: " + schemaName);
                Schema deserializedSchema = deserializedSchemaNavigator
                        .getSchemaByName(schemaName);
                Assert.assertNotNull(deserializedSchema);

                for (Table table : schema.getTables()) {
                    String tableName = table.getName();
                    logger.info("\t\tTable: " + tableName);
                    Table deserializedTable = deserializedSchema
                            .getTableByName(tableName);
                    Assert.assertNotNull(deserializedTable);

                    for (Column column : table.getColumns()) {
                        String columnName = column.getName();
                        logger.info("\t\t\tColumn: " + columnName);
                        Column deserializedColumn = deserializedTable
                                .getColumnByName(columnName);
                        Assert.assertNotNull(deserializedColumn);
                    }
                }
            }
        }
    }

	@Test
	public void testDeserializeSerializeAnalysisJob() {
		AnalysisJob deserializedAnalysisJob = ConfigurationSerializer
				.deserializeAnalysisJobFromXml(analysisJobXml,
						analyzerBeansConfigurationCsv);
		String serializedAnalysisJobXml = ConfigurationSerializer
				.serializeAnalysisJobToXml(analyzerBeansConfigurationCsv,
						deserializedAnalysisJob);
		String expected = analysisJobXml.replace('\t', ' ').replace('\n', ' ')
				.replace('\r', ' ').replace('\f', ' ');
		String updatedTagStart = "<updated-date>";
		String updatedTagEnd = "</updated-date>";
		expected = expected.substring(0, expected.indexOf(updatedTagStart)) + expected.substring(expected.indexOf(updatedTagEnd) + updatedTagEnd.length());
		serializedAnalysisJobXml = serializedAnalysisJobXml.replace('\t', ' ')
				.replace('\n', ' ').replace('\r', ' ').replace('\f', ' ');
		serializedAnalysisJobXml = serializedAnalysisJobXml.substring(0, serializedAnalysisJobXml.indexOf(updatedTagStart)) + serializedAnalysisJobXml.substring(serializedAnalysisJobXml.indexOf(updatedTagEnd) + updatedTagEnd.length());
		Assert.assertEquals(expected, serializedAnalysisJobXml);
	}

	private String hardcodedAnalysisJobXml() {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?> <job xmlns=\"http://eobjects.org/analyzerbeans/job/1.0\">     <job-metadata>         <updated-date>2014-02-14-08:00</updated-date>     </job-metadata>     <source>         <data-context ref=\"Country codes\"/>         <columns>             <column id=\"col_0\" path=\"countrycodes.csv.countrycodes.ISO 3166-2\" type=\"VARCHAR\"/>             <column id=\"col_1\" path=\"countrycodes.csv.countrycodes.ISO 3166-3\" type=\"VARCHAR\"/>         </columns>     </source>     <transformation>         <transformer>             <descriptor ref=\"Concatenator\"/>             <properties>                 <property name=\"Separator\" value=\"&lt;null&gt;\"/>             </properties>             <input ref=\"col_0\"/>             <input value=\"_\"/>             <input ref=\"col_1\"/>             <output name=\"concatenated\" id=\"col_2\"/>         </transformer>         <transformer>             <descriptor ref=\"Tokenizer\"/>             <properties>                 <property name=\"Delimiters\" value=\"[ ,_]\"/>                 <property name=\"Number of tokens\" value=\"2\"/>                 <property name=\"Token target\" value=\"COLUMNS\"/>             </properties>             <input ref=\"col_2\"/>             <output name=\"concatenated (token 1)\" id=\"col_3\"/>             <output name=\"concatenated (token 2)\" id=\"col_4\"/>         </transformer>     </transformation>     <analysis>         <analyzer>             <descriptor ref=\"String analyzer\"/>             <properties/>             <input ref=\"col_2\"/>             <input ref=\"col_0\"/>             <input ref=\"col_1\"/>             <input ref=\"col_3\"/>             <input ref=\"col_4\"/>         </analyzer>     </analysis> </job> ";
	}
}
