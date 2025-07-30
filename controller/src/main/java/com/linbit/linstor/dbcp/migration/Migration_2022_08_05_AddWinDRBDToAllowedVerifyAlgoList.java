package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2022.08.05.08.00",
    description = "Add WinDRBD to allowed verify algorithm list"
)
public class Migration_2022_08_05_AddWinDRBDToAllowedVerifyAlgoList extends LinstorMigration
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
            "UPDATE " + TBL_PROPS_CONTAINERS + " SET " + PROP_VALUE + "='crct10dif-pclmul;crct10dif-generic;" +
                "crc32c-intel;crc32c-generic;sha384-generic;sha512-generic;sha256-generic;md5-generic;windrbd'" +
                " WHERE " + PROPS_INSTANCE + "='/CTRLCFG' AND " + PROP_KEY + "='" + propKey + "'"
        );
    }
}
