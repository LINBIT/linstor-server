package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2026.04.08.09.00",
    description = "Fix Linstor/Drbd/auto-block-size property on controller level"
)
public class Migration_2026_04_08_FixAutoBlockSizeProperty extends LinstorMigration
{
    public static final String PROP = "Linstor/Drbd/auto-block-size";
    public static final String CTRL = "/CTRL";
    public static final String STLT = "/STLT";

    @Override
    public void migrate(Connection con, DbProduct dbProduct) throws Exception
    {
        try (
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT PROPS_INSTANCE, PROP_VALUE FROM PROPS_CONTAINERS " +
                    "WHERE PROP_KEY = '" + PROP + "'")
        )
        {
            @Nullable String oldVal = null;
            @Nullable String newVal = null;
            while (rs.next())
            {
                String instance = rs.getString("PROPS_INSTANCE");
                String val = rs.getString("PROP_VALUE");
                if (instance.equals(STLT))
                {
                    oldVal = val;
                }
                else if (instance.equals(CTRL))
                {
                    newVal = val;
                }
            }
            if (oldVal == null)
            {
                // noop. Regardless if newVal is properly set or not.
            }
            else if (newVal == null)
            {
                // move property from old instance to new (correct) instance
                stmt.execute(
                    "UPDATE PROPS_CONTAINERS SET PROPS_INSTANCE = '" + CTRL + "' " +
                        "WHERE PROPS_INSTANCE = '" + STLT + "' AND PROP_KEY = '" + PROP + "'");
            }
            else
            {
                // both exist. just delete the old one
                stmt.execute(
                    "DELETE FROM PROPS_CONTAINERS " +
                        "WHERE PROPS_INSTANCE = '" + STLT + "' AND PROP_KEY = '" + PROP + "'");
            }
        }
    }
}
