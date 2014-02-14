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
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileResource;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationSerializerTest {

	private AnalyzerBeansConfiguration analyzerBeansConfiguration;
	private String analysisJobXml;

	@Before
	public void setUp() {
		this.analyzerBeansConfiguration = buildAnalyzerBeansConfiguration();

		this.analysisJobXml = hardcodedAnalysisJobXml();
	}

	private AnalyzerBeansConfiguration buildAnalyzerBeansConfiguration() {
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
	public void testSerializeDeserializeDatastores() {
		String csv = ConfigurationSerializer
				.serializeAnalyzerBeansConfigurationToCsv(analyzerBeansConfiguration);
		AnalyzerBeansConfiguration deserialized = ConfigurationSerializer
				.deserializeDatastoresFromCsv(csv);
		for (String datastoreName : analyzerBeansConfiguration
				.getDatastoreCatalog().getDatastoreNames()) {
			Datastore datastore = analyzerBeansConfiguration
					.getDatastoreCatalog().getDatastore(datastoreName);
			Datastore deserializedDatastore = deserialized
					.getDatastoreCatalog().getDatastore(datastoreName);
			Assert.assertNotNull(deserializedDatastore);

			SchemaNavigator schemaNavigator = datastore.openConnection()
					.getSchemaNavigator();
			SchemaNavigator deserializedSchemaNavigator = deserializedDatastore
					.openConnection().getSchemaNavigator();
			for (Schema schema : schemaNavigator.getSchemas()) {
				Schema deserializedSchema = deserializedSchemaNavigator
						.getSchemaByName(schema.getName());
				Assert.assertNotNull(deserializedSchema);

				for (Table table : schema.getTables()) {
					Table deserializedTable = deserializedSchema
							.getTableByName(table.getName());
					Assert.assertNotNull(deserializedTable);

					for (Column column : table.getColumns()) {
						Column deserializedColumn = deserializedTable
								.getColumnByName(column.getName());
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
						analyzerBeansConfiguration);
		String serializedAnalysisJobXml = ConfigurationSerializer
				.serializeAnalysisJobToXml(analyzerBeansConfiguration,
						deserializedAnalysisJob);
		String expected = analysisJobXml.replace('\t', ' ').replace('\n', ' ')
				.replace('\r', ' ').replace('\f', ' ');
		serializedAnalysisJobXml = serializedAnalysisJobXml.replace('\t', ' ')
				.replace('\n', ' ').replace('\r', ' ').replace('\f', ' ');
		Assert.assertEquals(expected, serializedAnalysisJobXml);
	}

	private String hardcodedAnalysisJobXml() {
		return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?> <job xmlns=\"http://eobjects.org/analyzerbeans/job/1.0\">     <job-metadata>         <updated-date>2014-02-14-08:00</updated-date>     </job-metadata>     <source>         <data-context ref=\"Country codes\"/>         <columns>             <column id=\"col_0\" path=\"countrycodes.csv.countrycodes.ISO 3166-2\" type=\"VARCHAR\"/>             <column id=\"col_1\" path=\"countrycodes.csv.countrycodes.ISO 3166-3\" type=\"VARCHAR\"/>         </columns>     </source>     <transformation>         <transformer>             <descriptor ref=\"Concatenator\"/>             <properties>                 <property name=\"Separator\" value=\"&lt;null&gt;\"/>             </properties>             <input ref=\"col_0\"/>             <input value=\"_\"/>             <input ref=\"col_1\"/>             <output name=\"concatenated\" id=\"col_2\"/>         </transformer>         <transformer>             <descriptor ref=\"Tokenizer\"/>             <properties>                 <property name=\"Delimiters\" value=\"[ ,_]\"/>                 <property name=\"Number of tokens\" value=\"2\"/>                 <property name=\"Token target\" value=\"COLUMNS\"/>             </properties>             <input ref=\"col_2\"/>             <output name=\"concatenated (token 1)\" id=\"col_3\"/>             <output name=\"concatenated (token 2)\" id=\"col_4\"/>         </transformer>     </transformation>     <analysis>         <analyzer>             <descriptor ref=\"String analyzer\"/>             <properties/>             <input ref=\"col_2\"/>             <input ref=\"col_0\"/>             <input ref=\"col_1\"/>             <input ref=\"col_3\"/>             <input ref=\"col_4\"/>         </analyzer>     </analysis> </job> ";
	}
}
