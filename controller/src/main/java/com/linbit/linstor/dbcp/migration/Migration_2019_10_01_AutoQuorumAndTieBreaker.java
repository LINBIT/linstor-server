package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.sql.Connection;
import java.sql.PreparedStatement;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2019.10.01.01.01",
    description = "Enables auto-quorum and auto-add-tiebreaker on controller level"
)
public class Migration_2019_10_01_AutoQuorumAndTieBreaker extends LinstorMigration
{
    @Override
    public void migrate(Connection dbCon, DbProduct dbProduct) throws Exception
    {
        try (
            PreparedStatement stmt = dbCon.prepareStatement(
                "INSERT INTO PROPS_CONTAINERS (PROPS_INSTANCE, PROP_KEY, PROP_VALUE) VALUES(?, ?, ?)"
            );
        )
        {
            stmt.setString(1, "/CTRLCFG");
            stmt.setString(2, "DrbdOptions/auto-quorum");
            stmt.setString(3, "io-error");
            stmt.execute();

            stmt.setString(2, "DrbdOptions/auto-add-quorum-tiebreaker");
            stmt.setString(3, "True");
            stmt.execute();
        }
    }
}
