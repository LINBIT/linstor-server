package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.utils.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

@Migration(
    version = "2021.04.29.13.00",
    description = "Fix incorrectly written data after migration 2021.03.30.12.00"
)
public class Migration_2021_04_29_FixVarcharToTextUpgrade extends LinstorMigration
{

    private static final String TBL_RG = "RESOURCE_GROUPS";
    private static final String COL_NAME_POOL_NAME = "POOL_NAME";
    private static final String COL_NAM_NODE_NAME_LIST = "NODE_NAME_LIST";
    private static final String COL_NAM_DO_NOT_PLACE_WITH_RSC_REGEX = "DO_NOT_PLACE_WITH_RSC_REGEX";
    private static final String COL_NAM_DO_NOT_PLACE_WITH_RSC_LIST = "DO_NOT_PLACE_WITH_RSC_LIST";
    private static final String COL_NAM_REPLICAS_ON_SAME = "REPLICAS_ON_SAME";
    private static final String COL_NAM_REPLICAS_ON_DIFFERENT = "REPLICAS_ON_DIFFERENT";

    private static final String[] COL_NAMES_FOR_TYPE_CHANGE = new String[] {
        COL_NAME_POOL_NAME,
        COL_NAM_NODE_NAME_LIST,
        COL_NAM_DO_NOT_PLACE_WITH_RSC_REGEX,
        COL_NAM_DO_NOT_PLACE_WITH_RSC_LIST,
        COL_NAM_REPLICAS_ON_SAME,
        COL_NAM_REPLICAS_ON_DIFFERENT
    };
    /*
     * 1.12.0 migrated columns of RESOURCE_GROUPS from varchar to text, but the db drivers still
     * wrote byte[] to them instead of Strings. Reading those values causes exceptions
     */
    @Override
    public void migrate(Connection dbCon, DbProduct dbProductRef) throws Exception
    {
        final ObjectMapper objMapper = new ObjectMapper();
        try
        (
            PreparedStatement select = dbCon.prepareStatement(
                " SELECT " + StringUtils.join(", ", COL_NAMES_FOR_TYPE_CHANGE) +
                " FROM " + TBL_RG
            );
            ResultSet rs = select.executeQuery();
        )
        {
            while (rs.next())
            {
                ResultSetMetaData rsMd = rs.getMetaData();
                for (int idx = 0; idx < COL_NAMES_FOR_TYPE_CHANGE.length; idx++)
                {
                    String col = COL_NAMES_FOR_TYPE_CHANGE[idx];
                    int columnType = rsMd.getColumnType(idx + 1);

                    if (columnType == Types.BLOB)
                    {
                        List<String> jsonList = objMapper.readValue(
                            (byte[]) rs.getObject(COL_NAME_POOL_NAME),
                            List.class
                        );
                        rs.updateString(col, objMapper.writeValueAsString(jsonList));
                    }
                }
            }
        }
    }
}
