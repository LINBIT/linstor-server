package com.linbit.linstor.layer.snapshot;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Map;

@Singleton
public class CtrlSnapLayerDataFactory
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final SnapDrbdLayerHelper drbdLayerHelper;
    private final SnapLuksLayerHelper luksLayerHelper;
    private final SnapStorageLayerHelper storageLayerHelper;
    private final SnapNvmeLayerHelper nvmeLayerHelper;
    private final SnapWritecacheLayerHelper writecacheLayerHelper;
    private final SnapCacheLayerHelper cacheLayerHelper;
    private final SnapBCacheLayerHelper bcacheLayerHelper;

    @Inject
    public CtrlSnapLayerDataFactory(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        SnapDrbdLayerHelper drbdLayerHelperRef,
        SnapLuksLayerHelper luksLayerHelperRef,
        SnapStorageLayerHelper storageLayerHelperRef,
        SnapNvmeLayerHelper nvmeLayerHelperRef,
        SnapWritecacheLayerHelper writecacheLayerHelperRef,
        SnapCacheLayerHelper cacheLayerHelperRef,
        SnapBCacheLayerHelper bcacheLayerHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        drbdLayerHelper = drbdLayerHelperRef;
        luksLayerHelper = luksLayerHelperRef;
        storageLayerHelper = storageLayerHelperRef;
        nvmeLayerHelper = nvmeLayerHelperRef;
        writecacheLayerHelper = writecacheLayerHelperRef;
        cacheLayerHelper = cacheLayerHelperRef;
        bcacheLayerHelper = bcacheLayerHelperRef;
    }

    public void copyLayerData(
        Resource fromResource,
        Snapshot toSnapshot
    )
    {
        AbsRscLayerObject<Resource> rscData;
        try
        {
            rscData = fromResource.getLayerData(apiCtx);

            AbsRscLayerObject<Snapshot> snapData = copyRec(toSnapshot, rscData, null);
            toSnapshot.setLayerData(apiCtx, snapData);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "A database exception occurred while creating layer data"
                ),
                exc
            );
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_RSC_LAYER_ID,
                    "No TCP/IP port number could be allocated for the resource"
                )
                    .setCause("The pool of free TCP/IP port numbers is exhausted")
                    .setCorrection(
                        "- Adjust the TcpPortAutoRange controller configuration value to extend the range\n" +
                            "  of TCP/IP port numbers used for automatic allocation\n" +
                            "- Delete unused resource definitions that occupy TCP/IP port numbers from the range\n" +
                            "  used for automatic allocation\n"
                    ),
                exc
            );
        }
        catch (Exception exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
                ),
                exc
            );
        }
    }

    private AbsRscLayerObject<Snapshot> copyRec(
        Snapshot snapRef,
        AbsRscLayerObject<Resource> rscDataRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException
    {
        AbsSnapLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(rscDataRef.getLayerKind());

        AbsRscLayerObject<Snapshot> snapData = layerHelper.copySnapData(snapRef, rscDataRef, parentRef);

        for (AbsRscLayerObject<Resource> child : rscDataRef.getChildren())
        {
            snapData.getChildren().add(copyRec(snapRef, child, snapData));
        }
        return snapData;
    }

    public void restoreLayerData(
        RscLayerDataApi fromRscLayerDataApiRef,
        Snapshot toSnapshot,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    )
    {
        try
        {
            AbsRscLayerObject<Snapshot> snapData = restoreRec(
                fromRscLayerDataApiRef,
                toSnapshot,
                null,
                renameStorPoolMapRef,
                apiCallRc
            );
            toSnapshot.setLayerData(apiCtx, snapData);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_SQL,
                    "A database exception occurred while creating layer data"
                ),
                exc
            );
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating layer data"
                ),
                exc
            );
        }
    }

    private AbsRscLayerObject<Snapshot> restoreRec(
        RscLayerDataApi fromRscLayerDataApiRef,
        Snapshot toSnapshotRef,
        @Nullable AbsRscLayerObject<Snapshot> parentRef,
        Map<String, String> renameStorPoolMapRef,
        @Nullable ApiCallRc apiCallRc
    )
        throws AccessDeniedException, DatabaseException, ValueOutOfRangeException, ExhaustedPoolException,
        ValueInUseException, InvalidNameException
    {
        AbsSnapLayerHelper<?, ?, ?, ?> layerHelper = getLayerHelperByKind(fromRscLayerDataApiRef.getLayerKind());

        AbsRscLayerObject<Snapshot> snapData = layerHelper.restoreSnapData(
            toSnapshotRef,
            fromRscLayerDataApiRef,
            parentRef,
            renameStorPoolMapRef,
            apiCallRc
        );

        for (RscLayerDataApi child : fromRscLayerDataApiRef.getChildren())
        {
            snapData.getChildren().add(restoreRec(child, toSnapshotRef, snapData, renameStorPoolMapRef, apiCallRc));
        }
        return snapData;
    }

    private AbsSnapLayerHelper<?, ?, ?, ?> getLayerHelperByKind(
        DeviceLayerKind kind
    )
    {
        AbsSnapLayerHelper<?, ?, ?, ?> layerHelper;
        switch (kind)
        {
            case DRBD:
                layerHelper = drbdLayerHelper;
                break;
            case LUKS:
                layerHelper = luksLayerHelper;
                break;
            case NVME:
                layerHelper = nvmeLayerHelper;
                break;
            case STORAGE:
                layerHelper = storageLayerHelper;
                break;
            case WRITECACHE:
                layerHelper = writecacheLayerHelper;
                break;
            case CACHE:
                layerHelper = cacheLayerHelper;
                break;
            case BCACHE:
                layerHelper = bcacheLayerHelper;
                break;
            default:
                throw new ImplementationError("Unknown device layer kind '" + kind + "'");
        }
        return layerHelper;
    }

}
