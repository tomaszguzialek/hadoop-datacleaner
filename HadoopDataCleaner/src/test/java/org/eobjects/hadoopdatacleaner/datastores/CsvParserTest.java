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
package org.eobjects.hadoopdatacleaner.datastores;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputColumn;
import org.eobjects.analyzer.data.MockInputRow;
import org.junit.Before;
import org.junit.Test;

public class CsvParserTest {

    CsvParser csvParser;

    @Before
    public void setUp() {
        Collection<InputColumn<?>> sourceColumns = new ArrayList<InputColumn<?>>();
        sourceColumns.add(new MockInputColumn<String>("Country name"));
        sourceColumns.add(new MockInputColumn<String>("ISO 3166-2"));
        sourceColumns.add(new MockInputColumn<String>("ISO 3166-3"));
        sourceColumns.add(new MockInputColumn<String>("ISO Numeric"));
        sourceColumns.add(new MockInputColumn<String>("Linked to country"));
        sourceColumns.add(new MockInputColumn<String>("Synonym1"));
        sourceColumns.add(new MockInputColumn<String>("Synonym2"));
        this.csvParser = new CsvParser(sourceColumns);
    }

    @Test
    public void testParseHeaderRowAndDenmark() {
        Text csvLine = new Text(
                "Country name;ISO 3166-2;ISO 3166-3;ISO Numeric;Linked to country;Synonym1;Synonym2;Synonym3");

        MockInputRow expectedRow = new MockInputRow();
        expectedRow.put(new MockInputColumn<String>("Country name"), "Country name");
        expectedRow.put(new MockInputColumn<String>("ISO 3166-2"), "ISO 3166-2");
        expectedRow.put(new MockInputColumn<String>("ISO 3166-3"), "ISO 3166-3");
        expectedRow.put(new MockInputColumn<String>("ISO Numeric"), "ISO Numeric");
        expectedRow.put(new MockInputColumn<String>("Linked to country"), "Linked to country");
        expectedRow.put(new MockInputColumn<String>("Synonym1"), "Synonym1");
        expectedRow.put(new MockInputColumn<String>("Synonym2"), "Synonym2");

        InputRow actualRow = csvParser.prepareRow(csvLine);

        Iterator<InputColumn<?>> actualColumnIterator = actualRow.getInputColumns().iterator();
        for (InputColumn<?> expectedInputColumn : expectedRow.getInputColumns()) {
            InputColumn<?> actualInputColumn = actualColumnIterator.next();
            Assert.assertEquals(expectedInputColumn.getName(), actualInputColumn.getName());
            Assert.assertEquals(expectedRow.getValue(expectedInputColumn), actualRow.getValue(actualInputColumn));
        }

        csvLine = new Text("Denmark;DK;DNK;208;;Danmark;Danemark;");

        expectedRow = new MockInputRow();
        expectedRow.put(new MockInputColumn<String>("Country name"), "Denmark");
        expectedRow.put(new MockInputColumn<String>("ISO 3166-2"), "DK");
        expectedRow.put(new MockInputColumn<String>("ISO 3166-3"), "DNK");
        expectedRow.put(new MockInputColumn<String>("ISO Numeric"), "208");
        expectedRow.put(new MockInputColumn<String>("Linked to country"), "");
        expectedRow.put(new MockInputColumn<String>("Synonym1"), "Danmark");
        expectedRow.put(new MockInputColumn<String>("Synonym2"), "Danemark");
        
        actualRow = csvParser.prepareRow(csvLine);

        actualColumnIterator = actualRow.getInputColumns().iterator();
        for (InputColumn<?> expectedInputColumn : expectedRow.getInputColumns()) {
            InputColumn<?> actualInputColumn = actualColumnIterator.next();
            Assert.assertEquals(expectedInputColumn.getName(), actualInputColumn.getName());
            Assert.assertEquals(expectedRow.getValue(expectedInputColumn), actualRow.getValue(actualInputColumn));
        }
    }

}
