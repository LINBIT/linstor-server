package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.security.AccessDeniedException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescription;

public class ResourceDataUtils
{
    private ResourceDataUtils()
    {
    }

    public static void recalculateVolatileRscData(CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef, Resource rscRef)
    {
        try
        {
            ctrlRscLayerDataFactoryRef.recalculateVolatileRscData(rscRef);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "reclculating volatile properties of " + getRscDescription(rscRef),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }
}
