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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraParser {

    @SuppressWarnings("unused")
    private Logger logger = LoggerFactory.getLogger(CassandraParser.class);

    private Collection<InputColumn<?>> sourceColumns;

    public CassandraParser(Collection<InputColumn<?>> sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    public InputRow prepareRow(Map<String, ByteBuffer> columns) {
        MockInputRow row = new MockInputRow();

        for (Map.Entry<String, ByteBuffer> columnsEntry : columns.entrySet()) {
            String columnName = columnsEntry.getKey();
            String value;
            try {
                value = ByteBufferUtil.string(columnsEntry.getValue());
                for (Iterator<InputColumn<?>> sourceColumnsIterator = sourceColumns.iterator(); sourceColumnsIterator
                        .hasNext();) {
                    InputColumn<?> inputColumn = (InputColumn<?>) sourceColumnsIterator.next();
                    String shortName = inputColumn.getName().substring(inputColumn.getName().lastIndexOf('.') + 1);
                    if (shortName.equals(columnName)) {
                        row.put(inputColumn, value);
                        break;
                    }
                }
            } catch (CharacterCodingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return row;
    }

}
