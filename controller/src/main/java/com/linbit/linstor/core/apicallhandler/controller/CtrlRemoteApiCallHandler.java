package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.Remote.Flags;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.S3RemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRemoteApiCallHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final S3RemoteControllerFactory s3remoteFactory;
    private final RemoteRepository remoteRepository;

    @Inject
    public CtrlRemoteApiCallHandler(
        @SystemContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        ResponseConverter responseConverterRef,
        S3RemoteControllerFactory s3remoteFactoryRef,
        RemoteRepository remoteRepositoryRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        responseConverter = responseConverterRef;
        s3remoteFactory = s3remoteFactoryRef;
        remoteRepository = remoteRepositoryRef;
    }

    public List<S3RemotePojo> listS3()
    {
        ArrayList<S3RemotePojo> ret = new ArrayList<>();
        try
        {
            AccessContext pAccCtx = peerAccCtx.get();
            for (Entry<RemoteName, Remote> entry : remoteRepository.getMapForView(pAccCtx).entrySet())
            {
                if (entry.getValue() instanceof S3Remote)
                {
                    ret.add(((S3Remote) entry.getValue()).getApiData(pAccCtx, null, null));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            // ignore, we will return an empty list
        }
        return ret;
    }

    public Flux<ApiCallRc> createS3(
        String remoteName,
        String endpointRef,
        String bucketRef,
        String regionRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create s3 remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> createS3InTransaction(
                remoteName, endpointRef, bucketRef, regionRef, accessKeyRef, secretKeyRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createS3InTransaction(
        String remoteNameStr,
        String endpointRef,
        String bucketRef,
        String regionRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStr);
        if (ctrlApiDataLoader.loadRemote(remoteName, false) != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "A remote with the name '" + remoteNameStr +
                        "' already exists. Please use a different name or try to modify the remote instead."
                )
            );
        }
        S3Remote remote = null;

        try
        {
            remote = s3remoteFactory
                .create(peerAccCtx.get(), remoteName, endpointRef, bucketRef, regionRef, accessKeyRef, secretKeyRef);
            remoteRepository.put(apiCtx, remote);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getRemoteDescription(remoteNameStr),
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ImplementationError(exc);
        }

        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellite(remote);
    }

    public Flux<ApiCallRc> changeS3(
        String remoteName,
        String endpointRef,
        String bucketRef,
        String regionRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Modify s3 remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> changeS3InTransaction(
                remoteName, endpointRef, bucketRef, regionRef, accessKeyRef, secretKeyRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeS3InTransaction(
        String remoteNameStr,
        String endpointRef,
        String bucketRef,
        String regionRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStr);
        Remote remote = ctrlApiDataLoader.loadRemote(remoteName, true);
        if (!(remote instanceof S3Remote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "The remote with the name '" + remoteNameStr +
                        "' is not a s3 remote."
                )
            );
        }
        S3Remote s3remote = (S3Remote) remote;
        try
        {
            if (endpointRef != null && !endpointRef.isEmpty())
            {
                s3remote.setUrl(peerAccCtx.get(), endpointRef);
            }
            if (bucketRef != null && !bucketRef.isEmpty())
            {
                s3remote.setBucket(peerAccCtx.get(), bucketRef);
            }
            if (regionRef != null && !regionRef.isEmpty())
            {
                s3remote.setRegion(peerAccCtx.get(), regionRef);
            }
            if (accessKeyRef != null && !accessKeyRef.isEmpty())
            {
                s3remote.setAccessKey(peerAccCtx.get(), accessKeyRef);
            }
            if (secretKeyRef != null && !secretKeyRef.isEmpty())
            {
                s3remote.setSecretKey(peerAccCtx.get(), secretKeyRef);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "modify " + getRemoteDescription(remoteNameStr),
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellite(remote);
    }

    public Flux<ApiCallRc> delete(String remoteNameStrRef)
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteNameStrRef
        );

        return scopeRunner.fluxInTransactionalScope(
            "Delete remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> deleteInTransaction(remoteNameStrRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteInTransaction(String remoteNameStrRef)
    {
        Flux<ApiCallRc> flux;
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStrRef);
        Remote remote = ctrlApiDataLoader.loadRemote(remoteName, false);
        String remoteDescription = getRemoteDescription(remoteNameStrRef);

        if (remote == null)
        {
            flux = Flux.<ApiCallRc> just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND,
                    remoteDescription + " not found."
                )
            );
        }
        else
        {

            enableFlags(remote, Remote.Flags.DELETE);
            ctrlTransactionHelper.commit();
            ApiCallRcImpl responses = new ApiCallRcImpl();
            responses.addEntry(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, remoteDescription + " marked for deletion.")
                    .setDetails(remoteDescription + " UUID is: " + remote.getUuid().toString())
                    .build()
            );
            flux = Flux.<ApiCallRc> just(responses)
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellite(remote))
                .concatWith(deleteImpl(remote));
        }
        return flux;
    }

    private Flux<ApiCallRc> deleteImpl(Remote remoteRef)
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteRef.getName().displayValue
        );

        return scopeRunner.fluxInTransactionalScope(
            "Delete external file impl",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> deleteImplInTransaction(remoteRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteImplInTransaction(Remote remoteRef)
    {
        RemoteName remoteName = remoteRef.getName();
        String remoteDescription = getRemoteDescription(remoteName.displayValue);
        UUID uuid = remoteRef.getUuid();

        try
        {
            remoteRef.delete(peerAccCtx.get());
            remoteRepository.remove(apiCtx, remoteName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + remoteDescription,
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();

        ApiCallRcImpl.ApiCallRcEntry response = ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, remoteDescription + " deleted.")
            .setDetails(remoteDescription + " UUID was: " + uuid.toString())
            .build();
        return Flux.just(new ApiCallRcImpl(response));
    }

    private void enableFlags(Remote remoteRef, Flags... flags)
    {
        try
        {
            remoteRef.getFlags().enableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + getRemoteDescription(remoteRef.getName().displayValue),
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    public static ResponseContext makeRemoteContext(
        ApiOperation operation,
        String nameRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_REMOTE, nameRef);

        return new ResponseContext(
            operation,
            getRemoteDescription(nameRef),
            getRemoteDescriptionInline(nameRef),
            ApiConsts.MASK_REMOTE,
            objRefs
        );
    }

    public static String getRemoteDescription(String pathRef)
    {
        return "Remote: " + pathRef;
    }

    public static String getRemoteDescriptionInline(String pathRef)
    {
        return "remote '" + pathRef + "'";
    }

}
