package com.linbit.linstor.dbdrivers.sql.dump;

import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator;
import com.linbit.linstor.dbdrivers.DatabaseConstantsGenerator.Table;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.sql.dump.TableDump.DataRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SqlDump
{
    public static DbDump getDump(Connection con) throws DatabaseException
    {
        Map<String, TableDump> tables = new TreeMap<>();
        try
        {
            TreeMap<String, Table> extractedTables = DatabaseConstantsGenerator.extractTables(
                con,
                Collections.emptySet()
            ).objA;
            for (Table tbl : extractedTables.values())
            {
                List<DataRow> dataList = new ArrayList<>();
                String tblName = tbl.getName();

                try (
                    PreparedStatement select = con.prepareStatement("SELECT * FROM LINSTOR." + tblName);
                    ResultSet resultSet = select.executeQuery();
                )
                {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (resultSet.next())
                    {
                        Map<String, Object> valuesMap = new LinkedHashMap<>();
                        for (int clmIdx = 1; clmIdx <= columnCount; clmIdx++)
                        {
                            String clmName = metaData.getColumnName(clmIdx);
                            Object value = resultSet.getObject(clmIdx);
                            valuesMap.put(clmName, value);
                        }
                        dataList.add(new DataRow(valuesMap));
                    }
                }
                tables.put(tblName, new TableDump(tbl, dataList));
            }
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        return new DbDump(tables);
    }
}
