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

import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseParser {

    @SuppressWarnings("unused")
    private Logger logger = LoggerFactory.getLogger(HBaseParser.class);

    private Collection<InputColumn<?>> sourceColumns;

    public HBaseParser(Collection<InputColumn<?>> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public InputRow prepareRow(Result result) {
        MockInputRow row = new MockInputRow();
        for (KeyValue keyValue : result.raw()) {
            SplitKeyValue splitKeyValue = keyValue.split();
            String familyName = Bytes.toString(splitKeyValue.getFamily());
            String columnName = Bytes.toString(splitKeyValue.getQualifier());
            String value = Bytes.toString(splitKeyValue.getValue());
            for (Iterator<InputColumn<?>> sourceColumnsIterator = sourceColumns.iterator(); sourceColumnsIterator
                    .hasNext();) {
                InputColumn<?> inputColumn = (InputColumn<?>) sourceColumnsIterator.next();
                if (inputColumn.getName().equals(familyName + ":" + columnName)) {
                    row.put(inputColumn, value);
                    break;
                }
                
            }
        }
        return row;
    }

}
