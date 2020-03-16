package com.linbit.linstor.dbdrivers.sql.dump;

import java.util.Collections;
import java.util.Map;

public class DbDump
{
    public final Map<String, TableDump> tables;

    DbDump(Map<String, TableDump> tablesRef)
    {
        tables = Collections.unmodifiableMap(tablesRef);
    }

    public String serializeHuman()
    {
        StringBuilder sb = new StringBuilder();
        for (TableDump tbl : tables.values())
        {
            sb.append(tbl.serializeHuman());
            sb.append("\n");
        }
        return sb.toString().trim();
    }

    public String serializeJson()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (TableDump tbl : tables.values())
        {
            sb.append(tbl.serializeJson());
            sb.append(",");
        }
        sb.setLength(sb.length() - ",".length());
        sb.append("]");
        return sb.toString();
    }
}
