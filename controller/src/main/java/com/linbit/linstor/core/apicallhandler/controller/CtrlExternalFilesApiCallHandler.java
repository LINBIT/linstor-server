package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.ExternalFile.Flags;
import com.linbit.linstor.core.objects.ExternalFileControllerFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ExternalFileRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
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
import java.util.function.Predicate;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlExternalFilesApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;

    private final ExternalFileControllerFactory extFileFactory;
    private final ExternalFileRepository extFileRepository;
    private final ResourceDefinitionRepository rscDfnRepo;

    @Inject
    public CtrlExternalFilesApiCallHandler(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        ResponseConverter responseConverterRef,
        ExternalFileControllerFactory extFileFactoryRef,
        ExternalFileRepository extFileRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        responseConverter = responseConverterRef;
        extFileFactory = extFileFactoryRef;
        extFileRepository = extFileRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
    }

    public List<ExternalFilePojo> listFiles(Predicate<String> includeExtFileRef)
    {
        ArrayList<ExternalFilePojo> ret = new ArrayList<>();
        try
        {
            AccessContext pAccCtx = peerAccCtx.get();
            for (Entry<ExternalFileName, ExternalFile> entry : extFileRepository.getMapForView(pAccCtx).entrySet())
            {
                if (includeExtFileRef.test(entry.getKey().extFileName))
                {
                    ret.add(entry.getValue().getApiData(pAccCtx, null, null));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            // ignore, we will return an empty list
        }
        return ret;
    }

    /**
     * Checks if the given file-path is whitelisted in the stltConfig of the given node
     *
     * @param fileName
     * @param nodeName
     *
     * @return {@code true} if the file can be written<br/>
     * {@code false} if it can't be written, the node doesn't exist, the nodeName or fileName is invalid, or there is an
     * exception
     */
    public boolean checkFile(String fileName, String nodeName)
    {
        boolean allowed = false;
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.EXT_FILE_MAP, LockObj.NODES_MAP))
        {
            Node node = ctrlApiDataLoader.loadNode(nodeName, false);
            ExternalFileName extFileName = LinstorParsingUtils.asExtFileName(fileName);

            if (node != null)
            {
                allowed = CtrlExternalFilesHelper.isPathWhitelisted(extFileName, node, peerAccCtx.get());
            }
        }
        catch (AccessDeniedException | ApiRcException exc)
        {
            // ignore exc, return false
        }
        return allowed;
    }

    public Flux<ApiCallRc> set(String extFileNameStr, byte[] content)
    {
        ResponseContext context = makeExtFilesContext(
            ApiOperation.makeModifyOperation(),
            extFileNameStr
        );

        return scopeRunner.fluxInTransactionalScope(
            "Set external file",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.EXT_FILE_MAP),
            () -> setInTransaction(extFileNameStr, content)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setInTransaction(
        String extFileNameStr,
        byte[] contentRef
    )
    {
        ExternalFileName extFileName = LinstorParsingUtils.asExtFileName(extFileNameStr);
        ExternalFile extFile = ctrlApiDataLoader.loadExtFile(extFileName, false);

        try
        {
            if (extFile == null)
            {
                try
                {
                    checkValidContent(contentRef);
                    checkValidPath(extFileNameStr);

                    extFile = extFileFactory.create(peerAccCtx.get(), extFileName, contentRef);
                    extFileRepository.put(apiCtx, extFile);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ApiAccessDeniedException(
                        exc,
                        "create " + getExtFileDescription(extFileNameStr),
                        ApiConsts.FAIL_ACC_DENIED_EXT_FILE
                    );
                }
                catch (LinStorDataAlreadyExistsException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
            else
            {
                if (contentRef != null && contentRef.length > 0)
                {
                    checkValidContent(contentRef);
                    try
                    {
                        extFile.setContent(peerAccCtx.get(), contentRef);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ApiAccessDeniedException(
                            exc,
                            "modify " + getExtFileDescription(extFileNameStr),
                            ApiConsts.FAIL_ACC_DENIED_EXT_FILE
                        );
                    }
                }
            }
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellite(extFile);
    }


    private void checkValidContent(byte[] contentRef)
    {
        if (contentRef == null || contentRef.length == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_EXT_FILE,
                    "The content must not be null or empty"
                )
            );
        }
    }

    private void checkValidPath(String extFileNameStr)
    {
        if (extFileNameStr == null || extFileNameStr.length() == 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_EXT_FILE,
                    "The path must not be null or empty",
                    true
                )
            );
        }
        if (!extFileNameStr.startsWith("/"))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_EXT_FILE,
                    "The path must be absolute",
                    true
                )
            );
        }
    }

    public Flux<ApiCallRc> delete(String extFileNameStrRef)
    {
        ResponseContext context = makeExtFilesContext(
            ApiOperation.makeModifyOperation(),
            extFileNameStrRef
        );

        return scopeRunner.fluxInTransactionalScope(
            "Delete external file",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.EXT_FILE_MAP),
            () -> deleteInTransaction(extFileNameStrRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteInTransaction(String extFileNameStrRef)
    {
        Flux<ApiCallRc> flux;
        ExternalFileName extFileName = LinstorParsingUtils.asExtFileName(extFileNameStrRef);
        ExternalFile extFile = ctrlApiDataLoader.loadExtFile(extFileName, false);
        String extFileDescription = getExtFileDescription(extFileNameStrRef);

        if (extFile == null)
        {
            flux = Flux.<ApiCallRc>just(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND,
                    extFileDescription + " not found in LINSTOR database."
                )
            );
        }
        else
        {
            List<Flux<ApiCallRc>> cleanupFluxes = cleanupPropertyEntries(extFile);

            enableFlags(extFile, ExternalFile.Flags.DELETE);
            ctrlTransactionHelper.commit();
            ApiCallRcImpl responses = new ApiCallRcImpl();
            responses.addEntry(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, extFileDescription + " marked for deletion.")
                    .setDetails(extFileDescription + " UUID is: " + extFile.getUuid().toString())
                    .build()
            );
            flux = Flux.<ApiCallRc>just(responses)
                .concatWith(Flux.concat(cleanupFluxes))
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellite(extFile))
                .concatWith(deleteImpl(extFile));
        }
        return flux;
    }

    /*
     * For now, we only support rscDfn props. If that changes, this method needs to be extended as well!
     */
    private List<Flux<ApiCallRc>> cleanupPropertyEntries(ExternalFile extFileRef)
    {
        List<Flux<ApiCallRc>> fluxList = new ArrayList<>();
        try
        {
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(apiCtx).values())
            {
                Props rscDfnProps = rscDfn.getProps(apiCtx);
                boolean changed = CtrlExternalFilesHelper.removePath(rscDfnProps, extFileRef) != null;
                if (changed)
                {
                    fluxList.add(
                        ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, null).transform(
                            updateResponses -> CtrlResponseUtils.combineResponses(
                                errorReporter,
                                updateResponses,
                                rscDfn.getName(),
                                "Updated Resource definition {1} on {0}"
                            )
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return fluxList;
    }

    private Flux<ApiCallRc> deleteImpl(ExternalFile extFileRef)
    {
        ResponseContext context = makeExtFilesContext(
            ApiOperation.makeModifyOperation(),
            extFileRef.getName().extFileName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Delete external file impl",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.EXT_FILE_MAP),
            () -> deleteImplInTransaction(extFileRef)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteImplInTransaction(ExternalFile extFileRef)
    {
        ExternalFileName extFileName = extFileRef.getName();
        String extFileDescription = getExtFileDescription(extFileName.extFileName);
        UUID uuid = extFileRef.getUuid();

        try
        {
            extFileRef.delete(peerAccCtx.get());
            extFileRepository.remove(apiCtx, extFileName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + extFileDescription,
                ApiConsts.FAIL_ACC_DENIED_EXT_FILE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        ctrlTransactionHelper.commit();

        ApiCallRcImpl.ApiCallRcEntry response = ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, extFileDescription + " deleted.")
            .setDetails(extFileDescription + " UUID was: " + uuid.toString())
            .build();
        return Flux.just(new ApiCallRcImpl(response));
    }

    private void enableFlags(ExternalFile extFileRef, Flags... flags)
    {
        try
        {
            extFileRef.getFlags().enableFlags(peerAccCtx.get(), flags);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "delete " + getExtFileDescription(extFileRef.getName().extFileName),
                ApiConsts.FAIL_ACC_DENIED_EXT_FILE
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    public static ResponseContext makeExtFilesContext(
        ApiOperation operation,
        String pathRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_EXT_FILE, pathRef);

        return new ResponseContext(
            operation,
            getExtFileDescription(pathRef),
            getExtFileDescriptionInline(pathRef),
            ApiConsts.MASK_EXT_FILES,
            objRefs
        );
    }

    public static String getExtFileDescription(String pathRef)
    {
        return "External file: " + pathRef;
    }

    public static String getExtFileDescriptionInline(String pathRef)
    {
        return "external file '" + pathRef + "'";
    }
}
