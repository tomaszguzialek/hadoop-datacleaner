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
import java.util.Map.Entry;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputRow;

public class CsvParser {

    private Collection<InputColumn<?>> jobColumns;
    
    private Collection<Boolean> usedColumns;

    private String delimiter;

    public CsvParser(Collection<InputColumn<?>> jobColumns) {
        this(jobColumns, ",");
    }
    
    public CsvParser(Collection<InputColumn<?>> jobColumns, String delimiter) {
        this.jobColumns = jobColumns;
        this.delimiter = delimiter;
    }

    private void parseHeaderRow(Text csvLine) {
        if (usedColumns == null) {
            usedColumns = new ArrayList<Boolean>();

            String[] values = csvLine.toString().split(delimiter);

            for (String value : values) {
                Boolean found = false;
                for (Iterator<InputColumn<?>> jobColumnsIterator = jobColumns.iterator(); jobColumnsIterator.hasNext();) {
                    InputColumn<?> jobColumn = (InputColumn<?>) jobColumnsIterator.next();
                    String shortName = jobColumn.getName().substring(jobColumn.getName().lastIndexOf('.') + 1);
                    if (shortName.equals(value)) {
                        found = true;
                        break;
                    }
                }
                usedColumns.add(found);
            }
        }

    }

    public InputRow prepareRow(Text csvLine) {
        if (usedColumns == null)
            parseHeaderRow(csvLine);
        
        String[] values = csvLine.toString().split(";");

        Iterator<InputColumn<?>> jobColumnsIterator = jobColumns.iterator();
        Iterator<Boolean> usedColumnsIterator = usedColumns.iterator();

        MockInputRow row = new MockInputRow();
        for (String value : values) {
            Boolean used = usedColumnsIterator.next();
            if (used) {
                InputColumn<?> inputColumn = jobColumnsIterator.next();
                row.put(inputColumn, value);
            }
        }
        return row;
    }
    
    public static Text toCsvText(Iterable<SortedMapWritable> rows) {
        Text finalText = new Text();
        for (SortedMapWritable row : rows) {
            for (@SuppressWarnings("rawtypes")
            Iterator<Entry<WritableComparable, Writable>> iterator = row.entrySet().iterator(); iterator.hasNext();) {
                Text value = ((Text) iterator.next().getValue());
                finalText.set(finalText.toString() + value.toString());
                if (iterator.hasNext())
                    finalText.set(finalText.toString() + ";");
                else
                    finalText.set(finalText.toString());
            }
        }
        return finalText;
    }

}
