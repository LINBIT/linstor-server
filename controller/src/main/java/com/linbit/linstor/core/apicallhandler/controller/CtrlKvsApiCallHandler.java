package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.KeyValueStoreControllerFactory;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.inject.Provider;

@Singleton
public class CtrlKvsApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<AccessContext> apiCtx;
    private final KeyValueStoreRepository kvsRepo;
    private final KeyValueStoreControllerFactory kvsFactory;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    public CtrlKvsApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @ApiContext Provider<AccessContext> apiCtxRef,
        KeyValueStoreRepository kvsRepositoryRef,
        KeyValueStoreControllerFactory kvsFactoryRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        peerAccCtx = peerAccCtxRef;
        apiCtx = apiCtxRef;
        kvsRepo = kvsRepositoryRef;
        kvsFactory = kvsFactoryRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    private KeyValueStore create(AccessContext accCtx, KeyValueStoreName kvsName)
    {
        KeyValueStore kvs;
        try
        {
            kvs = kvsFactory.create(accCtx, kvsName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiRcException(
            ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_ACC_DENIED_NODE,
                "ObjProt of non-existing KeyValueStore denies access of registering the KeyValueStore in question."
            ),
            new LinStorException(
                "An accessDeniedException occurred during creation of a KSV (KeyValueStore). That means the " +
                    "ObjectProtection (of the non-existing KVS) denied access to the KVS. " +
                    "It is possible that someone has modified the database accordingly. Please " +
                    "file a bug report otherwise.",
                accDeniedExc
            ));
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_EXISTS_NODE,
                        "Registration of KeyValueStore '" + kvsName.displayValue + "' failed."
                    )
                    .setCause("A KeyValueStore with the specified name '" + kvsName.displayValue + "' already exists.")
                    .setCorrection("Specify another name for the KeyValueStore\n")
                    .setSkipErrorReport(true)
                    .build(),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }

        return kvs;
    }

    Set<KvsApi> listKvs()
    {
        Set<KvsApi> retMap = new HashSet<>();
        try
        {
            AccessContext accCtx = peerAccCtx.get();
            for (KeyValueStore kvs : kvsRepo.getMapForView(accCtx).values())
            {
                retMap.add(kvs.getApiData(accCtx, null, null));
            }
        }
        catch (Exception exc)
        {
            exc.printStackTrace();
            retMap = Collections.emptySet();
        }
        return retMap;
    }

    ApiCallRc modifyKvs(
        UUID kvsUuid,
        String kvsNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            requireKvsMapChangeAccess();
            KeyValueStore kvs = ctrlApiDataLoader.loadKvs(kvsNameStr, false);
            if (kvsUuid != null && kvs != null && !kvsUuid.equals(kvs.getUuid()))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UUID_KVS,
                        "UUID-check failed"
                    )
                );
            }

            AccessContext accCtx = peerAccCtx.get();
            if (kvs == null)
            {
                kvs = create(accCtx, LinstorParsingUtils.asKvsName(kvsNameStr));
                kvsRepo.put(apiCtx.get(), LinstorParsingUtils.asKvsName(kvsNameStr), kvs);
            }

            ctrlPropsHelper.addModifyDeleteUnconditional(
                kvs.getProps(accCtx),
                overrideProps,
                deletePropKeys,
                deleteNamespaces
            );

            if (kvs.getProps(accCtx).isEmpty())
            {
                kvsRepo.remove(accCtx, kvs.getName());
                kvs.delete(accCtx);
            }

            ctrlTransactionHelper.commit();

            apiCallRc.addEntry(
                "Successfully updated properties",
                ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.MODIFIED
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid key.")
                    .setCause("The key '" + exc.invalidKey + "' is invalid.")
                    .build(),
                exc
            );
        }
        catch (InvalidValueException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid value.")
                    .setCause("The value '" + exc.value + "' is invalid for key '" + exc.key + "'.")
                    .build(),
                exc
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access the given KeyValueStore",
                ApiConsts.FAIL_ACC_DENIED_KVS
            );
        }
        return apiCallRc;
    }

    ApiCallRc deleteKvs(UUID kvsUuidRef, String kvsNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        AccessContext accCtx = peerAccCtx.get();
        try
        {
            requireKvsMapChangeAccess();
            @Nullable KeyValueStore kvs = ctrlApiDataLoader.loadKvs(kvsNameStr, false);
            if (kvsUuidRef != null && kvs != null && !kvsUuidRef.equals(kvs.getUuid()))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UUID_KVS,
                        "UUID-check failed"
                    )
                );
            }

            if (kvs != null)
            {
                UUID kvsUuid = kvs.getUuid();
                String kvsDescription = firstLetterCaps(getKvsDescriptionInline(kvs));
                KeyValueStoreName kvsName = kvs.getName();

                kvs.delete(accCtx);
                kvsRepo.remove(accCtx, kvsName);
                ctrlTransactionHelper.commit();

                apiCallRc.addEntry(
                    ApiCallRcImpl
                        .entryBuilder(ApiConsts.DELETED, kvsDescription + " deleted.")
                        .setDetails(kvsDescription + " UUID was: " + kvsUuid.toString())
                        .build()
                );
            }
            else
            {
                apiCallRc.addEntry(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of kvs '" + kvsNameStr + "' had no effect."
                    )
                    .setCause("Kvs '" + kvsNameStr + "' does not exist.")
                    .build()
                );
            }
        }
        catch (DatabaseException exc)
        {
            apiCallRc.addEntry(
                    ResponseUtils.getSqlMsg("Persisting properties in instancename '" + kvsNameStr + "'"),
                    ApiConsts.FAIL_SQL
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access the given KeyValueStore",
                ApiConsts.FAIL_ACC_DENIED_KVS
            );
        }
        return apiCallRc;
    }

    public static String getKvsDescriptionInline(KeyValueStore kvs)
    {
        return getKvsDescriptionInline(kvs.getName().displayValue);
    }

    public static String getKvsDescriptionInline(String kvsNameStr)
    {
        return "kvs '" + kvsNameStr + "'";
    }

    private void requireKvsMapChangeAccess()
    {
        try
        {
            kvsRepo.requireAccess(peerAccCtx.get(), AccessType.CHANGE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                    accDeniedExc,
                    "change any keyValueStores",
                    ApiConsts.FAIL_ACC_DENIED_KVS
            );
        }
    }
}
