package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2024.04.19.10.00",
    description = "Update verify algorithm list"
)
public class Migration_2024_04_19_UpdateAllowedVerifyAlgoList extends LinstorMigration
{
    private static final String TBL_PROPS_CONTAINERS = "PROPS_CONTAINERS";
    private static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    private static final String PROP_KEY = "PROP_KEY";
    private static final String PROP_VALUE = "PROP_VALUE";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        final String propKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/auto-verify-algo-allowed-list";
        SQLUtils.runSql(
            connection,
            "UPDATE " + TBL_PROPS_CONTAINERS + " SET " + PROP_VALUE +
                "='crct10dif;crc32c;sha384;sha512;sha256;sha1;md5;windrbd'" +
                " WHERE " + PROPS_INSTANCE + "='/CTRLCFG' AND " + PROP_KEY + "='" + propKey + "'"
        );
    }
}
