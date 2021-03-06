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
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputColumn;
import org.eobjects.analyzer.data.MockInputRow;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HBaseParserTest {

    private HBaseParser hBaseParser;
    
    @Before
    public void setUp() {
        Collection<InputColumn<?>> sourceColumns = new ArrayList<InputColumn<?>>();
        sourceColumns.add(new MockInputColumn<String>("mainFamily:country_name"));
        sourceColumns.add(new MockInputColumn<String>("mainFamily:iso2"));
        sourceColumns.add(new MockInputColumn<String>("mainFamily:iso3"));
        hBaseParser = new HBaseParser(sourceColumns);
    }
    
    @Test
    public void testPrepareRow() {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        KeyValue keyValue = new KeyValue(Bytes.toBytes("Denmark"), Bytes.toBytes("mainFamily"),
                Bytes.toBytes("country_name"), Bytes.toBytes("Denmark"));
        keyValues.add(keyValue);
        keyValue = new KeyValue(Bytes.toBytes("Denmark"), Bytes.toBytes("mainFamily"),
                Bytes.toBytes("iso2"), Bytes.toBytes("DK"));
        keyValues.add(keyValue);
        keyValue = new KeyValue(Bytes.toBytes("Denmark"), Bytes.toBytes("mainFamily"),
                Bytes.toBytes("iso3"), Bytes.toBytes("DNK"));
        keyValues.add(keyValue);
        Result result = new Result(keyValues);
        
        MockInputRow expectedRow = new MockInputRow();
        expectedRow.put(new MockInputColumn<String>("mainFamily:country_name"), "Denmark");
        expectedRow.put(new MockInputColumn<String>("mainFamily:iso2"), "DK");
        expectedRow.put(new MockInputColumn<String>("mainFamily:iso3"), "DNK");
        
        InputRow actualRow = hBaseParser.prepareRow(result);
        
        Iterator<InputColumn<?>> actualColumnIterator = actualRow.getInputColumns().iterator();
        for (InputColumn<?> expectedInputColumn : expectedRow.getInputColumns()) {
            InputColumn<?> actualInputColumn = actualColumnIterator.next();
            Assert.assertEquals(expectedInputColumn.getName(), actualInputColumn.getName());
            Assert.assertEquals(expectedRow.getValue(expectedInputColumn), actualRow.getValue(actualInputColumn));
        }
    }

}
