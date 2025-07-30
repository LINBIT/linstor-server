package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.utils.ByteUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

@Migration(
    version = "2021.05.03.13.00",
    description = "Fix incorrectly written data after migration 2021.03.30.12.00 and 2021.04.29.*"
)
public class Migration_2021_05_03_FixVarcharToTextUpgrade2 extends LinstorMigration
{
    private static final String TBL_RG = "RESOURCE_GROUPS";
    private static final String COL_NAME_POOL_NAME = "POOL_NAME";
    private static final String COL_NAME_NODE_NAME_LIST = "NODE_NAME_LIST";
    // private static final String COL_NAME_DO_NOT_PLACE_WITH_RSC_REGEX = "DO_NOT_PLACE_WITH_RSC_REGEX";
    private static final String COL_NAME_DO_NOT_PLACE_WITH_RSC_LIST = "DO_NOT_PLACE_WITH_RSC_LIST";
    private static final String COL_NAME_REPLICAS_ON_SAME = "REPLICAS_ON_SAME";
    private static final String COL_NAME_REPLICAS_ON_DIFFERENT = "REPLICAS_ON_DIFFERENT";

    private static final String[] COL_NAMES_FOR_TYPE_CHANGE = new String[] {
        COL_NAME_POOL_NAME,
        COL_NAME_NODE_NAME_LIST,
        // COL_NAME_DO_NOT_PLACE_WITH_RSC_REGEX, // ...REGEX is not stored as List, but as String since the beginning.
        // no need to convert
        COL_NAME_DO_NOT_PLACE_WITH_RSC_LIST,
        COL_NAME_REPLICAS_ON_SAME,
        COL_NAME_REPLICAS_ON_DIFFERENT
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
                " SELECT * FROM " + TBL_RG, // ALL columns required for rs.updateRow()
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE
            );
            ResultSet rs = select.executeQuery();
        )
        {
            while (rs.next())
            {
                for (int idx = 0; idx < COL_NAMES_FOR_TYPE_CHANGE.length; idx++)
                {
                    String col = COL_NAMES_FOR_TYPE_CHANGE[idx];
                    if (rs.getObject(col) != null && !rs.wasNull())
                    {
                        try
                        {
                            List<String> jsonList = objMapper.readValue(
                                rs.getBytes(col),
                                List.class
                            );
                            rs.updateString(col, objMapper.writeValueAsString(jsonList));
                        }
                        catch (Exception exc)
                        {
                            try
                            {
                                // test if it is already expected type.
                                List<String> jsonList = objMapper.readValue(
                                    rs.getString(col),
                                    List.class
                                );
                            }
                            catch (Exception exc2)
                            {
                                String str = rs.getString(col);
                                if (str.startsWith("\\x"))
                                {
                                    String actualStr = new String(
                                        ByteUtils.hexToBytes(str.substring(2))// cut leading "\x"
                                    );

                                    // still try to parse this, just to make sure this is JSON
                                    List<String> jsonList = objMapper.readValue(actualStr, List.class);

                                    // actualStr and the result of objMapper.writeValueAsString "should" be the same.
                                    rs.updateString(col, objMapper.writeValueAsString(jsonList));
                                }
                            }
                        }
                    }
                }
                rs.updateRow();
            }
        }
    }
}
