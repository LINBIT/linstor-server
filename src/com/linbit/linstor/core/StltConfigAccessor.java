package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Named;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;

public class StltConfigAccessor
{
    private Props stltProps;

    @Inject
    public StltConfigAccessor(
        @Named(LinStor.SATELLITE_PROPS) Props stltPropsRef
    )
    {
        stltProps = stltPropsRef;
    }

    public boolean useDmStats()
    {
        String dmStatsStr = null;
        try
        {
            dmStatsStr = stltProps.getProp(ApiConsts.KEY_DMSTATS, ApiConsts.NAMESPC_STORAGE_DRIVER);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError("Hardcoded invalid property keys", exc);
        }
        return dmStatsStr != null && getAsBoolean(dmStatsStr);
    }

    private boolean getAsBoolean(String val)
    {
        return
            val.equalsIgnoreCase("true") ||
            val.equalsIgnoreCase("yes");
    }
}
