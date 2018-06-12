package com.linbit.linstor.core;

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
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;
import java.sql.SQLException;

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
        CtrlStltSerializer interComSerializer,
        CtrlObjectFactories objectFactories,
        Provider<TransactionMgr> transMgrProviderRef,
        AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        String defaultStorPoolNameRef,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef
        )
    {
        super(
            errorReporterRef,
            apiCtx,
            LinStorObject.VOLUME_DEFINITION,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef
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
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                String.format(
                    "A volume definition with the number %d already exists in resource definition '%s'.",
                    volNr.value,
                    rscDfn.getName().getDisplayName()
                ),
                ApiConsts.FAIL_EXISTS_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
        catch (MdException mdExc)
        {
            throw asExc(
                mdExc,
                String.format(
                    "The " + getObjectDescriptionInline() + " has an invalid size of '%d'. " +
                        "Valid sizes range from %d to %d.",
                    size,
                    MetaData.DRBD_MIN_NET_kiB,
                    MetaData.DRBD_MAX_kiB
                ),
                ApiConsts.FAIL_INVLD_VLM_SIZE
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw asExc(
                exc,
                String.format(
                    "The specified minor number '%d' is invalid.",
                    minorNr
                ),
                ApiConsts.FAIL_INVLD_MINOR_NR
            );
        }
        catch (ExhaustedPoolException exhaustedPoolExc)
        {
            throw asExc(
                exhaustedPoolExc,
                "Could not find free minor number",
                ApiConsts.FAIL_POOL_EXHAUSTED_MINOR_NR
            );
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
            throw asExc(
                invalidNameExc,
                "The given stor pool name '" + invalidNameExc.invalidName + "' is invalid",
                ApiConsts.FAIL_INVLD_STOR_POOL_NAME
            );
        }
        catch (LinStorException linStorExc)
        {
            throw asExc(
                linStorExc,
                "An exception occured while adjusting resources.",
                ApiConsts.FAIL_UNKNOWN_ERROR // TODO somehow find out if the exception is caused
                // by a missing storpool (not deployed yet?), and return a more meaningful RC
            );
        }
    }

}
