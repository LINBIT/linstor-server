package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.LinstorRemote;
import com.linbit.linstor.core.objects.LinstorRemoteControllerFactory;
import com.linbit.linstor.core.objects.Remote;
import com.linbit.linstor.core.objects.S3Remote;
import com.linbit.linstor.core.objects.S3RemoteControllerFactory;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRemoteApiCallHandler
{
    private static final Pattern LINSTOR_URL_PATTERN = Pattern.compile("(https?://)?([^:]+)(:[0-9]+)?");

    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final ResponseConverter responseConverter;
    private final S3RemoteControllerFactory s3remoteFactory;
    private final LinstorRemoteControllerFactory linstorRemoteFactory;
    private final RemoteRepository remoteRepository;
    private final EncryptionHelper encryptionHelper;
    private final ErrorReporter errorReporter;

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
        LinstorRemoteControllerFactory linstorRemoteFactoryRef,
        RemoteRepository remoteRepositoryRef,
        EncryptionHelper encryptionHelperRef,
        ErrorReporter errorReporterRef
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
        linstorRemoteFactory = linstorRemoteFactoryRef;
        remoteRepository = remoteRepositoryRef;
        encryptionHelper = encryptionHelperRef;
        errorReporter = errorReporterRef;
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

    public List<LinstorRemotePojo> listLinstor()
    {
        ArrayList<LinstorRemotePojo> ret = new ArrayList<>();
        try
        {
            AccessContext pAccCtx = peerAccCtx.get();
            for (Entry<RemoteName, Remote> entry : remoteRepository.getMapForView(pAccCtx).entrySet())
            {
                if (entry.getValue() instanceof LinstorRemote)
                {
                    ret.add(((LinstorRemote) entry.getValue()).getApiData(pAccCtx, null, null));
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
            byte[] accessKey = encryptionHelper.encrypt(accessKeyRef);
            byte[] secretKey = encryptionHelper.encrypt(secretKeyRef);
            remote = s3remoteFactory
                .create(peerAccCtx.get(), remoteName, endpointRef, bucketRef, regionRef, accessKey, secretKey);
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
        catch (LinStorException exc)
        {
            errorReporter.reportError(exc);
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Encrypting the access key or secret key failed."
                )
            );
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
                byte[] accessKey;
                try
                {
                    accessKey = encryptionHelper.encrypt(accessKeyRef);
                }
                catch (LinStorException exc)
                {
                    errorReporter.reportError(exc);
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                            "Encrypting the access key failed."
                        )
                    );
                }
                s3remote.setAccessKey(peerAccCtx.get(), accessKey);
            }
            if (secretKeyRef != null && !secretKeyRef.isEmpty())
            {
                byte[] secretKey;
                try
                {
                    secretKey = encryptionHelper.encrypt(secretKeyRef);
                }
                catch (LinStorException exc)
                {
                    errorReporter.reportError(exc);
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                            "Encrypting the secret key failed."
                        )
                    );
                }
                s3remote.setSecretKey(peerAccCtx.get(), secretKey);
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

    public Flux<ApiCallRc> createLinstor(
        String remoteNameRef,
        String urlRef,
        String passphraseRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteNameRef
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create linstor remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> createLinstorInTransaction(
                remoteNameRef,
                urlRef,
                passphraseRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createLinstorInTransaction(
        String remoteNameRef,
        String urlRef,
        String passphraseRef
    )
    {
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameRef);
        if (ctrlApiDataLoader.loadRemote(remoteName, false) != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "A remote with the name '" + remoteNameRef +
                        "' already exists. Please use a different name or try to modify the remote instead."
                )
            );
        }
        LinstorRemote remote = null;

        try
        {
            byte[] encryptedTargetPassphrase = null;
            if (passphraseRef != null)
            {
                encryptedTargetPassphrase = encryptionHelper.encrypt(passphraseRef);
            }

            remote = linstorRemoteFactory.create(
                peerAccCtx.get(),
                remoteName,
                createUrlWithDefaults(urlRef),
                encryptedTargetPassphrase
            );
            remoteRepository.put(apiCtx, remote);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getRemoteDescription(remoteNameRef),
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
        catch (LinStorException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating linstor remote object"
                ),
                exc
            );
        }

        ctrlTransactionHelper.commit();
        return Flux.just(
            new ApiCallRcImpl(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.CREATED,
                    "Linstor remote successfully created"
                )
            )
        );
    }

    private URL createUrlWithDefaults(String urlRef)
    {
        URL url;

        Matcher matcher = LINSTOR_URL_PATTERN.matcher(urlRef);
        if (!matcher.find())
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Given URL '" + urlRef + "' did not match expected pattern of " + LINSTOR_URL_PATTERN.pattern()
                )
            );
        }

        String protocol = matcher.group(1);
        String domain = matcher.group(2);
        String port = matcher.group(3);

        int dfltPort = CtrlConfig.DEFAULT_HTTP_REST_PORT;
        if (protocol == null || protocol.isEmpty()) {
            protocol = "http://";
        }else if (protocol.equals("https://")) {
            dfltPort = CtrlConfig.DEFAULT_HTTPS_REST_PORT;
        }

        if (port == null || port.isEmpty()) {
            port = ":" + Integer.toString(dfltPort);
        }

        try
        {
            url = new URL(protocol + domain + port);
        }
        catch (MalformedURLException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while parsing the given URL"
                ),
                exc
            );
        }
        return url;
    }

    public Flux<ApiCallRc> changeLinstor(
        String remoteName,
        String urlStrRef,
        String passphraseRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Modify linstor remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> changeLinstorInTransaction(
                remoteName,
                urlStrRef,
                passphraseRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeLinstorInTransaction(
        String remoteNameStr,
        String urlStrRef,
        String passphraseRef
    )
    {
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStr);
        Remote remote = ctrlApiDataLoader.loadRemote(remoteName, true);
        if (!(remote instanceof LinstorRemote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "The remote with the name '" + remoteNameStr +
                        "' is not a s3 remote."
                )
            );
        }
        LinstorRemote linstorRemote = (LinstorRemote) remote;
        try
        {
            if (urlStrRef != null && !urlStrRef.isEmpty())
            {
                linstorRemote.setUrl(apiCtx, createUrlWithDefaults(urlStrRef));
            }
            if (passphraseRef != null && !passphraseRef.isEmpty())
            {
                linstorRemote.setEncryptedTargetPassphase(peerAccCtx.get(), encryptionHelper.encrypt(passphraseRef));
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
        catch (LinStorException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while creating linstor remote object"
                ),
                exc
            );
        }

        ctrlTransactionHelper.commit();
        return Flux.just(
            new ApiCallRcImpl(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.MODIFIED,
                    "Linstor remote successfully updated"
                )
            )
        );
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

    private void enableFlags(Remote remoteRef, Remote.Flags... flags)
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
