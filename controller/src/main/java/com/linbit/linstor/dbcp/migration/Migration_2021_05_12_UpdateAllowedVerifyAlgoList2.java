package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.SQLUtils;

import java.sql.Connection;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2021.05.12.09.00",
    description = "Update allowed verify algorithm list and fix incorrect controller disable"
)
public class Migration_2021_05_12_UpdateAllowedVerifyAlgoList2 extends LinstorMigration
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
                "sha384-generic;sha512-generic;sha256-generic;md5-generic'" +
                " WHERE " + PROPS_INSTANCE + "='/CTRLCFG' AND " + PROP_KEY + "='" + propKey + "'"
        );
        // migrate controller auto-verify-algo-disable to correct ctrlconf
        SQLUtils.runSql(
            connection,
            "UPDATE " + TBL_PROPS_CONTAINERS + " SET " + PROPS_INSTANCE + "='/CTRLCFG'" +
                " WHERE " + PROPS_INSTANCE + "='STLTCFG' AND " + PROP_KEY + "='DrbdOptions/auto-verify-algo-disable'"
        );
    }
}
