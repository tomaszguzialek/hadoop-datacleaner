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

	public static AnalyzerBeansConfiguration build(String analysisJobXml)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {
		return build(analysisJobXml, getDefaultDescriptorProvider());
	}

	public static AnalyzerBeansConfiguration build(String analysisJobXml,
			DescriptorProvider descriptorProvider)
			throws XPathExpressionException, ParserConfigurationException,
			SAXException, IOException {

		String datastoreName = getDatastoreName(analysisJobXml);
		String[] fullyQualifiedSourceColumnNames = getFullyQualifiedSourceColumnNames(analysisJobXml);
		String[] sourceColumnNames = getSourceColumnNames(fullyQualifiedSourceColumnNames);
		String schemaName = getSchemaName(fullyQualifiedSourceColumnNames[0]);
		String[] tableNames = getTableNames(fullyQualifiedSourceColumnNames);

		return build(datastoreName, schemaName, tableNames, sourceColumnNames,
				descriptorProvider);
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
		Datastore datastore = new PojoDatastore(datastoreName, schemaName,
				tableDataProviders);

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

	private static String[] getFullyQualifiedSourceColumnNames(
			String analysisJobXml) throws XPathExpressionException,
			UnsupportedEncodingException, SAXException, IOException,
			ParserConfigurationException {
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
	
	private static String[] getSourceColumnNames(
			String[] fullyQualifiedSourceColumnNames) {
		List<String> sourceColumnNames = new ArrayList<String>();
		for (String fullyQualifiedSourceColumnName : fullyQualifiedSourceColumnNames) {
			String[] split = fullyQualifiedSourceColumnName.split("\\.");
			assert split.length > 2;
			sourceColumnNames.add(split[2]);
		}
		return sourceColumnNames.toArray(new String[sourceColumnNames.size()]);
	}

	private static String getSchemaName(String fullyQualifiedSourceColumnName) {
		String[] split = fullyQualifiedSourceColumnName.split("\\.");
		assert split.length > 2;
		return split[0];
	}
	
	private static String[] getTableNames(
			String[] fullyQualifiedSourceColumnNames) {
		List<String> tableNames = new ArrayList<String>();
		for (String fullyQualifiedSourceColumnName : fullyQualifiedSourceColumnNames) {
			String[] split = fullyQualifiedSourceColumnName.split("\\.");
			assert split.length > 2;
			String tableName = split[1];
			if (!tableNames.contains(tableName)) {
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
