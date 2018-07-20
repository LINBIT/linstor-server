package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;
import java.sql.SQLException;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;

/**
 * Common API call handler base class for operations that create volume definitions.
 */
abstract class CtrlVlmDfnCrtApiCallHandler extends AbsApiCallHandler
{
    private final String defaultStorPoolName;
    private final VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;

    CtrlVlmDfnCrtApiCallHandler(
        ErrorReporter errorReporterRef,
        AccessContext apiCtx,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        String defaultStorPoolNameRef,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef
    )
    {
        super(
            errorReporterRef,
            apiCtx,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef
        );

        defaultStorPoolName = defaultStorPoolNameRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
    }

    protected VolumeDefinitionData createVlmDfnData(
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
            vlmDfn = volumeDefinitionDataFactory.getInstance(
                accCtx,
                rscDfn,
                volNr,
                minorNr,
                size,
                vlmDfnInitFlags,
                true,
                true
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
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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

    protected void adjustRscVolumes(Resource rsc)
    {
        try
        {
            rsc.adjustVolumes(apiCtx, defaultStorPoolName);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME,
                "The given stor pool name '" + invalidNameExc.invalidName + "' is invalid"
            ), invalidNameExc);
        }
        catch (LinStorException linStorExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_UNKNOWN_ERROR,
                "An exception occured while adjusting resources."
            ), linStorExc);
        }
    }

}
