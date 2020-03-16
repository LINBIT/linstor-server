package com.linbit.linstor.dbdrivers.sql.dump;

import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.Column;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.Table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class TableDump
{
    private static final String DELIMITER = " ;; ";

    public final List<DataRow> rows;
    private final Table tbl;

    TableDump(Table tblRef, List<DataRow> dataListRef)
    {
        tbl = tblRef;
        rows = Collections.unmodifiableList(dataListRef);
    }

    public String serializeHuman()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Table ").append(tbl.getName()).append(": \n");

        for (Column clm : tbl.getColumns())
        {
            sb.append("\t").append(clm.getName()).append(" ")
                .append(clm.getSqlType());
            if (!clm.isNullable())
            {
                sb.append(" NOT NULL");
            }
            if (clm.isPk())
            {
                sb.append(" PRIMARY KEY");
            }
            sb.append(";\n");
        }

        sb.append("Data:\n");
        for (DataRow row : rows)
        {
            for (Object cell : row.values.values())
            {
                sb.append(cell).append(DELIMITER);
            }
            sb.setLength(sb.length() - DELIMITER.length());
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    public String serializeJson()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
            .append("\"name\":\"").append(tbl.getName()).append("\",")
            .append("\"columns\":[");

        for (Column clm : tbl.getColumns())
        {
            sb.append("{\"name\":\"").append(clm.getName())
                .append("\",\"type\":\"").append(clm.getSqlType())
                .append("\",\"nullable\":").append(clm.isNullable())
                .append(",\"primary_key\":").append(clm.isPk())
                .append("},");
        }
        sb.setLength(sb.length() - ",".length());

        sb.append("],\"data\":[");
        for (DataRow row : rows)
        {
            sb.append("{");
            for (Entry<String, Object> entry : row.values.entrySet())
            {
                sb.append("\"").append(entry.getKey()).append("\":");
                Object val = entry.getValue();
                boolean primitiveValue = val instanceof Boolean || val instanceof Integer || val instanceof Double ||
                    val instanceof Long || val instanceof Float || val == null;
                if (!primitiveValue)
                {
                    sb.append("\"");
                }
                sb.append(Objects.toString(val).replaceAll("\"", "\\\\\""));
                if (!primitiveValue)
                {
                    sb.append("\"");
                }
                sb.append(",");
            }
            sb.setLength(sb.length() - ",".length());
            sb.append("},");
        }
        if (!rows.isEmpty())
        {
            sb.setLength(sb.length() - ",".length());
        }
        sb.append("]}");
        return sb.toString();
    }

    public static class DataRow
    {
        public final Map<String, Object> values;

        DataRow(Map<String, Object> valuesRef)
        {
            values = Collections.unmodifiableMap(valuesRef);
        }
    }
}
