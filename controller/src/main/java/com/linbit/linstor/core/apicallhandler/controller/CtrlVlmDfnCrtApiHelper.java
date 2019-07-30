package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;

@Singleton
class CtrlVlmDfnCrtApiHelper
{
    private final AccessContext apiCtx;
    private final VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;

    @Inject
    CtrlVlmDfnCrtApiHelper(
        @ApiContext AccessContext apiCtxRef,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef
    )
    {
        apiCtx = apiCtxRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
    }

    VolumeDefinitionData createVlmDfnData(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        VolumeNumber volNr,
        Integer minorNr,
        long size,
        VlmDfnFlags[] vlmDfnInitFlags
    )
    {
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = volumeDefinitionDataFactory.create(
                accCtx,
                rscDfn,
                volNr,
                minorNr,
                size,
                vlmDfnInitFlags
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getVlmDfnDescriptionInline(rscDfn, volNr),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_VLM_DFN, String.format(
                "A volume definition with the number %d already exists in resource definition '%s'.",
                volNr.value,
                rscDfn.getName().getDisplayName()
            )), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (MdException mdExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_VLM_SIZE, String.format(
                "The " + getVlmDfnDescriptionInline(rscDfn, volNr) + " has an invalid size of '%d'. " +
                    "Valid sizes range from %d to %d.",
                size,
                MetaData.DRBD_MIN_NET_kiB,
                MetaData.DRBD_MAX_kiB
            )), mdExc);
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_MINOR_NR, String.format(
                "The specified minor number '%d' is invalid.",
                minorNr
            )), exc);
        }
        catch (ExhaustedPoolException exhaustedPoolExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_MINOR_NR,
                "Could not find free minor number"
            ), exhaustedPoolExc);
        }
        return vlmDfn;
    }
}
