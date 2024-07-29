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
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CtrlSecurityObjects;
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
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemoteControllerFactory;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemoteControllerFactory;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.core.objects.remotes.S3RemoteControllerFactory;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.ScheduleBackupService;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.UuidUtils;

import com.linbit.linstor.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.SdkClientException;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlRemoteApiCallHandler
{
    private static final Pattern LINSTOR_URL_PATTERN = Pattern.compile("(https?://)?([^:]+)(:[0-9]+)?");
    private static final Pattern PATTERN_AWS_REGION_FROM_AZ = Pattern.compile("([^-]+-[^-]+-.)");

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
    private final EbsRemoteControllerFactory ebsRemoteFactory;
    private final RemoteRepository remoteRepository;
    private final EncryptionHelper encryptionHelper;
    private final BackupToS3 backupHandler;
    private final CtrlSecurityObjects ctrlSecObj;
    private final NodeRepository nodeRepo;

    private final ScheduleBackupService scheduleService;
    private final BackupInfoManager backupInfoMgr;

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
        EbsRemoteControllerFactory ebsRemoteFactoryRef,
        RemoteRepository remoteRepositoryRef,
        EncryptionHelper encryptionHelperRef,
        BackupToS3 backupHandlerRef,
        CtrlSecurityObjects ctrlSecObjRef,
        ScheduleBackupService scheduleServiceRef,
        NodeRepository nodeRepoRef,
        BackupInfoManager backupInfoMgrRef
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
        ebsRemoteFactory = ebsRemoteFactoryRef;
        remoteRepository = remoteRepositoryRef;
        encryptionHelper = encryptionHelperRef;
        backupHandler = backupHandlerRef;
        ctrlSecObj = ctrlSecObjRef;
        scheduleService = scheduleServiceRef;
        nodeRepo = nodeRepoRef;
        backupInfoMgr = backupInfoMgrRef;
    }

    public List<S3RemotePojo> listS3()
    {
        final AccessContext pAccCtx = peerAccCtx.get();
        return listGeneric(
            S3Remote.class,
            remote -> remote.getApiData(pAccCtx, null, null)
        );
    }

    public List<LinstorRemotePojo> listLinstor()
    {
        final AccessContext pAccCtx = peerAccCtx.get();
        return listGeneric(
            LinstorRemote.class,
            remote -> remote.getApiData(pAccCtx, null, null)
        );
    }

    public List<EbsRemotePojo> listEbs()
    {
        final AccessContext pAccCtx = peerAccCtx.get();
        return listGeneric(
            EbsRemote.class,
            remote -> remote.getApiData(pAccCtx, null, null)
        );
    }

    @SuppressWarnings("unchecked")
    private <RET_TYPE, REMOTE_CLASS extends AbsRemote> List<RET_TYPE> listGeneric(
        Class<REMOTE_CLASS> clazz,
        ExceptionThrowingFunction<REMOTE_CLASS, RET_TYPE, AccessDeniedException> remoteToApiDataFunc
    )
    {
        ArrayList<RET_TYPE> ret = new ArrayList<>();
        try
        {
            AccessContext pAccCtx = peerAccCtx.get();
            for (Entry<RemoteName, AbsRemote> entry : remoteRepository.getMapForView(pAccCtx).entrySet())
            {
                AbsRemote remote = entry.getValue();
                if (clazz.isInstance(remote))
                {
                    ret.add(remoteToApiDataFunc.accept((REMOTE_CLASS) remote));
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
        String secretKeyRef,
        boolean usePathStyleRef
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
                remoteName, endpointRef, bucketRef, regionRef, accessKeyRef, secretKeyRef, usePathStyleRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createS3InTransaction(
        String remoteNameStr,
        String endpointRef,
        String bucketRef,
        String regionRef,
        String accessKeyRef,
        String secretKeyRef,
        boolean usePathStyleRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        List<String> missingParams = new ArrayList<>();
        addIfStringNullOrEmpty(remoteNameStr, "remote_name", missingParams);
        addIfStringNullOrEmpty(endpointRef, "endpoint", missingParams);
        addIfStringNullOrEmpty(bucketRef, "bucket", missingParams);
        addIfStringNullOrEmpty(regionRef, "region", missingParams);
        addIfStringNullOrEmpty(accessKeyRef, "access_key", missingParams);
        addIfStringNullOrEmpty(secretKeyRef, "secret_key", missingParams);
        int missingCt = missingParams.size();
        if (missingCt > 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The remote could not be created because the following required " +
                        (missingCt == 1 ? "parameter is" : "parameters are") +
                        " missing: " + missingParams
                )
            );
        }
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStr);

        checkRemoteNameAvailable(remoteName);

        try
        {
            byte[] accessKey = encryptionHelper.encrypt(accessKeyRef);
            byte[] secretKey = encryptionHelper.encrypt(secretKeyRef);
            S3Remote remote = s3remoteFactory
                .create(peerAccCtx.get(), remoteName, endpointRef, bucketRef, regionRef, accessKey, secretKey);
            remoteRepository.put(apiCtx, remote);

            if (usePathStyleRef)
            {
                remote.getFlags().enableFlags(peerAccCtx.get(), AbsRemote.Flags.S3_USE_PATH_STYLE);
            }

            // check if url and keys work together
            byte[] masterKey = ctrlSecObj.getCryptKey();
            backupHandler.listObjects("", remote, peerAccCtx.get(), masterKey);

            ctrlTransactionHelper.commit();
            responses.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.CREATED | ApiConsts.MASK_REMOTE, "Remote created"));

            return Flux
                .<ApiCallRc>just(responses)
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(remote));
        }
        catch (SdkClientException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "The remote could not be reached with the given parameters and therefore wasn't created.\n" +
                        "Please check for spelling errors and that you have the correct access-key and secret-key.\n" +
                        "For more information on the error, please check the error-report."
                ),
                exc
            );
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
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Encrypting the access key or secret key failed."
                ),
                exc
            );
        }
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
        S3Remote s3remote = loadRemote(remoteName, S3Remote.class, "s3");
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
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                            "Encrypting the access key failed."
                        ),
                        exc
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
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                            "Encrypting the secret key failed."
                        ),
                        exc
                    );
                }
                s3remote.setSecretKey(peerAccCtx.get(), secretKey);
            }
            byte[] masterKey = ctrlSecObj.getCryptKey();
            backupHandler.listObjects("", s3remote, peerAccCtx.get(), masterKey);
        }
        catch (SdkClientException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "The remote could not be reached with the given parameters and therefore wasn't created.\n" +
                        "Please check for spelling errors and that you have the correct access-key and secret-key.\n" +
                        "For more information on the error, please check the error-report."
                ),
                exc
            );
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
        return ctrlSatelliteUpdateCaller.updateSatellites(s3remote);
    }

    public Flux<ApiCallRc> createLinstor(
        String remoteNameRef,
        String urlRef,
        String passphraseRef,
        String remoteClusterId
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
                passphraseRef,
                remoteClusterId
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createLinstorInTransaction(
        String remoteNameRef,
        String urlRef,
        String passphraseRef,
        String remoteClusterIdStr
    )
    {
        List<String> missingParams = new ArrayList<>();
        addIfStringNullOrEmpty(remoteNameRef, "remote_name", missingParams);
        addIfStringNullOrEmpty(urlRef, "url", missingParams);
        int missingCt = missingParams.size();
        if (missingCt > 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The remote could not be created because the following required " +
                        (missingCt == 1 ? "parameter is" : "parameters are") +
                        " missing: " + missingParams
                )
            );
        }
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameRef);
        checkRemoteNameAvailable(remoteName);
        LinstorRemote remote = null;

        UUID remoteClusterId = null;
        if (remoteClusterIdStr != null && !remoteClusterIdStr.isEmpty())
        {
            if (!UuidUtils.isUuid(remoteClusterIdStr))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_CONF,
                        "The given cluster id is not a valid UUID"
                    )
                );
            }
            remoteClusterId = UUID.fromString(remoteClusterIdStr);
        }

        try
        {
            byte[] encryptedTargetPassphrase = null;
            if (passphraseRef != null)
            {
                encryptedTargetPassphrase = encryptionHelper.encrypt(passphraseRef);
            }

            // TODO: check if remoteClusterId (if given) is unique. We could use
            // CtrlBackupL2LDstApiCallHandler#loadLinstorRemote for that
            remote = linstorRemoteFactory.create(
                peerAccCtx.get(),
                remoteName,
                createUrlWithDefaults(urlRef),
                encryptedTargetPassphrase,
                remoteClusterId
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
        if (protocol == null || protocol.isEmpty())
        {
            protocol = "http://";
        }
        else
        if (protocol.equals("https://"))
        {
            dfltPort = CtrlConfig.DEFAULT_HTTPS_REST_PORT;
        }

        if (port == null || port.isEmpty())
        {
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
        String passphraseRef,
        String clusterIidRef
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
                passphraseRef,
                clusterIidRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeLinstorInTransaction(
        String remoteNameStr,
        String urlStrRef,
        String passphraseRef,
        String clusterIidRef
    )
    {
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStr);
        LinstorRemote linstorRemote = loadRemote(remoteName, LinstorRemote.class, "linstor");
        try
        {
            if (urlStrRef != null && !urlStrRef.isEmpty())
            {
                linstorRemote.setUrl(peerAccCtx.get(), createUrlWithDefaults(urlStrRef));
            }
            if (passphraseRef != null && !passphraseRef.isEmpty())
            {
                linstorRemote.setEncryptedRemotePassphase(peerAccCtx.get(), encryptionHelper.encrypt(passphraseRef));
            }
            if (clusterIidRef != null && !clusterIidRef.isEmpty())
            {
                if (!UuidUtils.isUuid(clusterIidRef))
            {
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_CONF,
                            "The given cluster id is not a valid UUID"
                        )
                    );
                }
                linstorRemote.setClusterId(peerAccCtx.get(), UUID.fromString(clusterIidRef));
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

    public Flux<ApiCallRc> createEbs(
        String remoteNameStrRef,
        @Nullable String endpointRef,
        @Nullable String regionRef,
        String availabilityZoneRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteNameStrRef
        );

        return scopeRunner.fluxInTransactionalScope(
            "Create EBS remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> createEbsInTransaction(
                remoteNameStrRef,
                endpointRef,
                regionRef,
                availabilityZoneRef,
                accessKeyRef,
                secretKeyRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createEbsInTransaction(
        String remoteNameStrRef,
        @Nullable String endpointRef,
        @Nullable String regionRef,
        String availabilityZoneRef,
        String accessKeyRef,
        String secretKeyRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        List<String> missingParams = new ArrayList<>();
        addIfStringNullOrEmpty(remoteNameStrRef, "remote_name", missingParams);
        addIfStringNullOrEmpty(availabilityZoneRef, "availability_zone", missingParams);
        addIfStringNullOrEmpty(accessKeyRef, "access_key", missingParams);
        addIfStringNullOrEmpty(secretKeyRef, "secret_key", missingParams);
        int missingCt = missingParams.size();
        if (missingCt > 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_BACKUP_CONFIG | ApiConsts.MASK_BACKUP,
                    "The remote could not be created because the following required " +
                        (missingCt == 1 ? "parameter is" : "parameters are") +
                        " missing: " + missingParams
                )
            );
        }
        RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStrRef);
        checkRemoteNameAvailable(remoteName);

        final String region = buildRegion(regionRef, availabilityZoneRef);
        final String endpoint = buildEndpoing(endpointRef, region);

        EbsRemote remote;
        try
        {
            byte[] encryptedAccessKey = encryptionHelper.encrypt(accessKeyRef);
            byte[] encryptedSecretKey = encryptionHelper.encrypt(secretKeyRef);
            remote = ebsRemoteFactory.create(
                peerAccCtx.get(),
                remoteName,
                0,
                new URL(endpoint),
                region,
                availabilityZoneRef,
                encryptedAccessKey,
                encryptedSecretKey
            );
            remote.setDecryptedAccessKey(apiCtx, accessKeyRef);
            remote.setDecryptedSecretKey(apiCtx, secretKeyRef);
            remoteRepository.put(apiCtx, remote);

            // TODO perform check if accessKey and secretKey are correct if possible

            ctrlTransactionHelper.commit();
            responses.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.CREATED | ApiConsts.MASK_REMOTE, "Remote created"));
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "create " + getRemoteDescription(remoteNameStrRef),
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
                    ApiConsts.FAIL_UNKNOWN_ERROR | ApiConsts.MASK_BACKUP,
                    "Encrypting the access key or secret key failed."
                ),
                exc
            );
        }
        catch (MalformedURLException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_INVLD_CONF,
                        "End point '" + endpoint + "' is invalid."
                    )
                    .setSkipErrorReport(true)
                    .build(),
                exc
            );
        }

        return Flux.<ApiCallRc>just(responses)
            .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(remote));
    }

    private String buildRegion(String regionRef, String availabilityZoneRef)
    {
        final String region;
        if (regionRef != null)
        {
            region = regionRef;
        }
        else
        {
            Matcher matcher = PATTERN_AWS_REGION_FROM_AZ.matcher(availabilityZoneRef);
            if (!matcher.find())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_MISSING_EBS_TARGET,
                        "Cannot construct region. You have to specify the region",
                        true
                    )
                );
            }
            region = matcher.group(1);
        }
        return region;
    }

    private String buildEndpoing(String endpointRef, String regionRef)
    {
        final String endpoint;
        if (endpointRef != null)
        {
            endpoint = endpointRef;
        }
        else
        {
            endpoint = "https://ec2." + regionRef + ".amazonaws.com";
        }
        return endpoint;
    }

    public Flux<ApiCallRc> changeEbs(
        String remoteNameRef,
        @Nullable String endpointRef,
        @Nullable String regionRef,
        @Nullable String availabilityZoneRef,
        @Nullable String accessKeyRef,
        @Nullable String secretKeyRef
    )
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteNameRef
        );

        return scopeRunner.fluxInTransactionalScope(
            "Modify EBS remote",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.REMOTE_MAP),
            () -> changeEbsInTransaction(
                remoteNameRef,
                endpointRef,
                regionRef,
                availabilityZoneRef,
                accessKeyRef,
                secretKeyRef
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> changeEbsInTransaction(
        String remoteNameStrRef,
        @Nullable String endpointRef,
        @Nullable String regionRef,
        @Nullable String availabilityZoneRef,
        @Nullable String accessKeyRef,
        @Nullable String secretKeyRef
    )
    {
        final ApiCallRcImpl responses = new ApiCallRcImpl(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.MODIFIED,
                "EBS remote successfully updated"
            )
        );
        final RemoteName remoteName = LinstorParsingUtils.asRemoteName(remoteNameStrRef);
        final EbsRemote ebsRemote = loadRemote(remoteName, EbsRemote.class, "ebs");

        try
        {
            final AccessContext pAccCtx = peerAccCtx.get();
            final String changedEndpoint;
            final String changedRegion;
            if (availabilityZoneRef != null && !availabilityZoneRef.isEmpty())
            {
                ebsRemote.setAvailabilityZone(pAccCtx, availabilityZoneRef);
                changedRegion = buildRegion(regionRef, availabilityZoneRef);
                if (!Objects.equals(changedRegion, regionRef))
                {
                    responses.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            "Automatically updated region to " + changedRegion + " due to changed availability zone"
                        )
                    );
                }
            }
            else
            {
                changedRegion = regionRef;
            }

            if (changedRegion != null && !changedRegion.isEmpty())
            {
                ebsRemote.setRegion(pAccCtx, changedRegion);
                changedEndpoint = buildEndpoing(endpointRef, changedRegion);
                if (!Objects.equals(changedEndpoint, endpointRef))
                {
                    responses.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.MODIFIED,
                            "Automatically updated endpoint to " + changedEndpoint + " due to changed region"
                        )
                    );
                }
            }
            else
            {
                changedEndpoint = endpointRef;
            }

            if (changedEndpoint != null && !changedEndpoint.isEmpty())
            {
                ebsRemote.setUrl(pAccCtx, new URL(changedEndpoint));
            }
            if (accessKeyRef != null && !accessKeyRef.isEmpty())
            {
                ebsRemote.setEncryptedAccessKey(pAccCtx, encryptionHelper.encrypt(accessKeyRef));
                ebsRemote.setDecryptedAccessKey(pAccCtx, accessKeyRef);
            }
            if (secretKeyRef != null && !secretKeyRef.isEmpty())
            {
                ebsRemote.setEncryptedSecretKey(pAccCtx, encryptionHelper.encrypt(secretKeyRef));
                ebsRemote.setDecryptedSecretKey(pAccCtx, secretKeyRef);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "modify " + getRemoteDescription(remoteNameStrRef),
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (MalformedURLException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_INVLD_CONF,
                        "End point '" + endpointRef + "' is invalid."
                    )
                    .setSkipErrorReport(true)
                    .build(),
                exc
            );
        }
        catch (LinStorException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An exception occurred while modifying linstor remote object"
                ),
                exc
            );
        }

        ctrlTransactionHelper.commit();
        return Flux.<ApiCallRc>just(responses)
            .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(ebsRemote));
    }

    /**
     * Checks if there are in-progress shipments to this remote and if...
     * <list>
     * <ul>
     * ...there are, sets {@link AbsRemote.Flags#MARK_DELETED}: this remote will be deleted as soon as all in-progress
     * shipments are finished or aborted. While this is set, new shipments to this remote cannot be started.
     * </ul>
     * <ul>
     * ...there aren't, sets {@link AbsRemote.Flags#DELETE}: this remote will be deleted
     * </ul>
     * </list>
     *
     * @param remoteNameStrRef
     *
     * @return
     */
    public Flux<ApiCallRc> delete(String remoteNameStrRef)
    {
        ResponseContext context = makeRemoteContext(
            ApiOperation.makeModifyOperation(),
            remoteNameStrRef
        );
        // take extra locks because schedule-related props might need to be deleted as well
        return scopeRunner.fluxInTransactionalScope(
            "Delete remote",
            lockGuardFactory.buildDeferred(
                LockType.WRITE, LockObj.REMOTE_MAP, LockObj.RSC_DFN_MAP, LockObj.RSC_GRP_MAP, LockObj.CTRL_CONFIG
            ),
            () -> {
                @Nullable AbsRemote remote = loadRemote(remoteNameStrRef);
                Flux<ApiCallRc> flux;
                if (remote == null)
                {
                    flux = Flux.<ApiCallRc>just(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.WARN_NOT_FOUND,
                            getRemoteDescription(remoteNameStrRef) + " not found."
                        )
                    );
                }
                else
                {
                    flux = deleteInTransaction(remote);
                }
                return flux;
            }
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private void checkRemoteNameAvailable(RemoteName remoteName)
    {
        if (ctrlApiDataLoader.loadRemote(remoteName, false) != null)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "A remote with the name '" + remoteName.displayValue +
                        "' already exists. Please use a different name or try to modify the remote instead."
                )
            );
        }
    }

    @SuppressWarnings("unchecked")
    private <REMOTE_TYPE extends AbsRemote> REMOTE_TYPE loadRemote(
        RemoteName remoteNameRef,
        Class<REMOTE_TYPE> remoteTypeClassRef,
        String remoteTypeDescrRef
    )
    {
        AbsRemote remote = ctrlApiDataLoader.loadRemote(remoteNameRef, true);
        if (!remoteTypeClassRef.isInstance(remote))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_REMOTE,
                    "The remote with the name '" + remoteNameRef.displayValue +
                        "' is not a " + remoteTypeDescrRef + " remote."
                )
            );
        }
        return (REMOTE_TYPE) remote;
    }

    private @Nullable AbsRemote loadRemote(String remoteNameStr)
    {
        return loadRemote(LinstorParsingUtils.asRemoteName(remoteNameStr));
    }

    private @Nullable AbsRemote loadRemote(RemoteName remoteName)
    {
        return ctrlApiDataLoader.loadRemote(remoteName, false);
    }

    private Flux<ApiCallRc> deleteInTransaction(AbsRemote remote) throws ImplementationError
    {
        Flux<ApiCallRc> flux;

        String remoteDescription = getRemoteDescription(remote.getName().displayValue);
        ensureEbsRemoteUnusedPrivileged(remote);
        ApiCallRcImpl responses = new ApiCallRcImpl();
        Flux<ApiCallRc> postUpdateStltFlux;
        try
        {
            scheduleService.removeTasks(remote, peerAccCtx.get());
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "while trying to remove props",
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        backupInfoMgr.deleteFromQueue(remote);
        if (!backupInfoMgr.hasRemoteInProgressBackups(remote.getName()))
        {
            enableFlags(remote, AbsRemote.Flags.DELETE);
            if (remote instanceof S3Remote)
            {
                backupHandler.deleteRemoteFromCache((S3Remote) remote);
            }
            ctrlTransactionHelper.commit();
            responses.addEntry(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, remoteDescription + " marked for deletion.")
                    .setDetails(remoteDescription + " UUID is: " + remote.getUuid().toString())
                    .build()
            );

            postUpdateStltFlux = deleteImpl(remote);
        }
        else
        {
            enableFlags(remote, AbsRemote.Flags.MARK_DELETED);
            ctrlTransactionHelper.commit();
            responses.addEntry(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.MODIFIED,
                        remoteDescription +
                            " prepared for deletion. Waiting for ongoing shipments to complete or abort."
                    )
                    .setDetails(remoteDescription + " UUID is: " + remote.getUuid().toString())
                    .build()
            );
            postUpdateStltFlux = Flux.empty();
        }
        flux = Flux.<ApiCallRc>just(responses);
        if (!(remote instanceof LinstorRemote))
        {
            // satellites do not know about any linstor-remote, so updating them here would break stuff
            // (as in, the satellite will ignore the update and not answer -> timeout)
            flux = flux.concatWith(ctrlSatelliteUpdateCaller.updateSatellites(remote));
        }
        return flux.concatWith(postUpdateStltFlux);
    }

    private void ensureEbsRemoteUnusedPrivileged(AbsRemote remoteRef)
    {
        try
        {
            if (remoteRef instanceof EbsRemote)
            {
                final String remoteNameDispValue = remoteRef.getName().displayValue;
                for (Node node : nodeRepo.getMapForView(apiCtx).values())
                {
                    Iterator<StorPool> storPoolIt = node.iterateStorPools(apiCtx);
                    while (storPoolIt.hasNext())
                    {
                        StorPool storPool = storPoolIt.next();
                        String ebsRemoteName = storPool.getProps(apiCtx).getProp(
                            ApiConsts.KEY_REMOTE,
                            ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS
                        );
                        if (ebsRemoteName != null && ebsRemoteName.equalsIgnoreCase(remoteNameDispValue))
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.simpleEntry(
                                    ApiConsts.FAIL_IN_USE,
                                    "Remote " + remoteNameDispValue +
                                        " cannot be deleted as a storage pool still uses it"
                                )
                                    .setCorrection("Delete the storage pool first")
                                    .setSkipErrorReport(true)
                            );
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<ApiCallRc> deleteImpl(AbsRemote remoteRef)
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

    private Flux<ApiCallRc> deleteImplInTransaction(AbsRemote remoteRef)
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

    private void enableFlags(AbsRemote remoteRef, AbsRemote.Flags... flags)
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

    private void addIfStringNullOrEmpty(String str, String description, List<String> missingParams)
    {
        if (str == null || str.isEmpty())
        {
            missingParams.add(description);
        }
    }

    public Flux<ApiCallRc> cleanupRemotesIfNeeded(Set<RemoteName> remotes)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        for (RemoteName remoteName : remotes)
        {
            String remoteNameStr = remoteName.displayValue;
            ResponseContext context = makeRemoteContext(
                ApiOperation.makeModifyOperation(),
                remoteNameStr
            );
            // take extra locks because schedule-related props might need to be deleted as well
            ret = ret.concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Delete remote",
                    lockGuardFactory.buildDeferred(
                        LockType.WRITE,
                        LockObj.REMOTE_MAP,
                        LockObj.RSC_DFN_MAP,
                        LockObj.RSC_GRP_MAP,
                        LockObj.CTRL_CONFIG
                    ),
                    () ->
                    cleanupIfNeededInTransaction(remoteNameStr)
                ).transform(responses -> responseConverter.reportingExceptions(context, responses))
            );
        }
        return ret;
    }

    private Flux<ApiCallRc> cleanupIfNeededInTransaction(String remoteNameStr) throws ImplementationError
    {
        AbsRemote remote = loadRemote(remoteNameStr);
        Flux<ApiCallRc> flux = Flux.empty();
        try
        {
            if (remote == null)
            {
                flux = Flux.<ApiCallRc>just(
                    ApiCallRcImpl.singleApiCallRc(
                        ApiConsts.WARN_NOT_FOUND,
                        getRemoteDescription(remoteNameStr) + " not found."
                    )
                );
            }
            else if (remote.getFlags().isSet(peerAccCtx.get(), AbsRemote.Flags.MARK_DELETED))
            {
                flux = deleteInTransaction(remote);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Cannot access Flags of " + remoteNameStr,
                ApiConsts.FAIL_ACC_DENIED_REMOTE
            );
        }
        return flux;
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
