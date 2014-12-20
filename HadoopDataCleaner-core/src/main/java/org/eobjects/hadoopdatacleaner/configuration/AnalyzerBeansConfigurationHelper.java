package org.eobjects.hadoopdatacleaner.configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfigurationImpl;
import org.eobjects.analyzer.connection.Datastore;
import org.eobjects.analyzer.connection.DatastoreCatalog;
import org.eobjects.analyzer.connection.DatastoreCatalogImpl;
import org.eobjects.analyzer.connection.PojoDatastore;
import org.eobjects.analyzer.descriptors.ClasspathScanDescriptorProvider;
import org.eobjects.analyzer.descriptors.DescriptorProvider;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class AnalyzerBeansConfigurationHelper {

	private static final String[] DEFAULT_PACKAGES = new String[] { "org.eobjects" };

	public static AnalyzerBeansConfiguration build(String analysisJobXml,
			String inputTableName, String outputTableName)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {
		return build(analysisJobXml, inputTableName, outputTableName,
				getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String analysisJobXml,
			String inputTableName, String outputTableName,
			DescriptorProvider descriptorProvider)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {

		String[] jobSourceColumnNames = getJobSourceColumnNames(analysisJobXml);

		String datastoreName = getDatastoreName(analysisJobXml);
		String schemaName = getSchemaName(jobSourceColumnNames[0]);
		if (schemaName == null) {
			// FIXME: Get the parent directory
			schemaName = datastoreName;
		}
		String[] tableNames;
		if (hasTableName(jobSourceColumnNames[0])) {
			tableNames = getJobTableNames(jobSourceColumnNames);
		} else {
			tableNames = new String[1];
			tableNames[0] = inputTableName;
		}
		String[] sourceColumnNames = prepareSourceColumnNames(schemaName,
				tableNames, jobSourceColumnNames);

		return build(datastoreName, schemaName, tableNames, sourceColumnNames,
				descriptorProvider);
	}

	private static boolean hasTableName(String jobSourceColumnName) {
		String[] split = jobSourceColumnName.split("\\.");
		return split.length > 1;
	}

	public static AnalyzerBeansConfiguration build(String datastoreName,
			String schemaName, String[] tableNames, String[] columnNames) {
		return build(datastoreName, schemaName, tableNames, columnNames,
				getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String datastoreName,
			String schemaName, String[] tableNames, String[] columnNames,
			DescriptorProvider descriptorProvider) {
		List<TableDataProvider<?>> tableDataProviders = new ArrayList<TableDataProvider<?>>();
		for (String tableName : tableNames) {
			SimpleTableDef inputTableDef = new SimpleTableDef(tableName,
					columnNames);
			tableDataProviders.add(new ArrayTableDataProvider(inputTableDef,
					new ArrayList<Object[]>()));
		}
		Datastore datastore;
		if (schemaName != null) {
			datastore = new PojoDatastore(datastoreName, schemaName,
					tableDataProviders);
		} else {
			datastore = new PojoDatastore(datastoreName, tableDataProviders);
		}

		DatastoreCatalog datastoreCatalog = new DatastoreCatalogImpl(datastore);

		return new AnalyzerBeansConfigurationImpl().replace(datastoreCatalog)
				.replace(descriptorProvider);
	}

	private static String getDatastoreName(String analysisJobXml)
			throws ParserConfigurationException, SAXException, IOException,
			XPathExpressionException {
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
		Document document = documentBuilder.parse(new ByteArrayInputStream(
				analysisJobXml.getBytes("utf-8")));

		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression xPathExpression = xPath
				.compile("/job/source/data-context[@ref]");

		Node dataContextNode = (Node) xPathExpression.evaluate(document,
				XPathConstants.NODE);
		Node refAttributeNode = dataContextNode.getAttributes().getNamedItem(
				"ref");
		String datastoreName = refAttributeNode.getNodeValue();
		return datastoreName;
	}

	private static String[] getJobSourceColumnNames(String analysisJobXml)
			throws XPathExpressionException, UnsupportedEncodingException,
			SAXException, IOException, ParserConfigurationException {
		DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance()
				.newDocumentBuilder();
		Document document = documentBuilder.parse(new ByteArrayInputStream(
				analysisJobXml.getBytes("utf-8")));

		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression xPathExpression = xPath
				.compile("/job/source/columns/column[@path]");

		NodeList columnNodes = (NodeList) xPathExpression.evaluate(document,
				XPathConstants.NODESET);

		List<String> columnNames = new ArrayList<String>();
		for (int i = 0; i < columnNodes.getLength(); i++) {
			Node columnNode = columnNodes.item(i);
			Node pathAttributeNode = columnNode.getAttributes().getNamedItem(
					"path");
			columnNames.add(pathAttributeNode.getNodeValue());
		}
		return columnNames.toArray(new String[columnNodes.getLength()]);
	}

	private static String[] prepareSourceColumnNames(String schemaName,
			String[] tableNames, String[] sourceColumnNames) {
		if (!hasTableName(sourceColumnNames[0])) {
			return sourceColumnNames;
		} else {
			List<String> strippedSourceColumnNames = new ArrayList<String>();
			for (String sourceColumnName : sourceColumnNames) {
				String[] splitColumnName = sourceColumnName.split("\\.");
				strippedSourceColumnNames.add(splitColumnName[splitColumnName.length - 1]);
			}
			return strippedSourceColumnNames
					.toArray(new String[strippedSourceColumnNames.size()]);
		}
	}

	private static String getSchemaName(String fullyQualifiedSourceColumnName) {
		String[] split = fullyQualifiedSourceColumnName.split("\\.");
		if (split.length > 2) {
			return split[0];
		} else {
			// The fully-qualified name has only table name and no schema name
			return null;
		}
	}

	private static String[] getJobTableNames(
			String[] fullyQualifiedSourceColumnNames) {
		List<String> tableNames = new ArrayList<String>();
		for (String fullyQualifiedSourceColumnName : fullyQualifiedSourceColumnNames) {
			String[] split = fullyQualifiedSourceColumnName.split("\\.");
			String tableName = null;
			if (split.length == 4) {
				// the table names is actually a filename and it contains a dot and extensions
				tableName = split[1] + "." + split[2];
			} else if (split.length == 3) {
				tableName = split[1];
			} else if (split.length == 2) {
				tableName = split[0];
			}
			if ((tableName != null) && (!tableNames.contains(tableName))) {
				tableNames.add(tableName);
			}
		}
		return tableNames.toArray(new String[tableNames.size()]);
	}

	private static ClasspathScanDescriptorProvider getDefaultDescriptorProvider() {
		ClasspathScanDescriptorProvider descriptorProvider = new ClasspathScanDescriptorProvider();
		for (String packageName : DEFAULT_PACKAGES) {
			descriptorProvider.scanPackage(packageName, true);
		}
		return descriptorProvider;
	}

}
