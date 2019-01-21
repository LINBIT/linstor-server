package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.InvalidNameException;
import com.linbit.linstor.KeyValueStoreData;
import com.linbit.linstor.KeyValueStoreDataControllerFactory;
import com.linbit.linstor.KeyValueStoreName;
import com.linbit.linstor.KeyValueStoreRepository;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.inject.Provider;

@Singleton
public class CtrlKvsApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<AccessContext> apiCtx;
    private final KeyValueStoreRepository kvsRepo;
    private final KeyValueStoreDataControllerFactory kvsFactory;

    @Inject
    public CtrlKvsApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @ApiContext Provider<AccessContext> apiCtxRef,
        KeyValueStoreRepository kvsRepositoryRef,
        KeyValueStoreDataControllerFactory kvsFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        peerAccCtx = peerAccCtxRef;
        apiCtx = apiCtxRef;
        kvsRepo = kvsRepositoryRef;
        kvsFactory = kvsFactoryRef;
    }

    public ApiCallRc modifyProps(
        String kvsNameStr,
        Map<String, String> modProps,
        List<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            KeyValueStoreName kvsName = new KeyValueStoreName(kvsNameStr);

            AccessContext accCtx = peerAccCtx.get();
            KeyValueStoreData kvs = kvsRepo.get(accCtx, kvsName);

            if (kvs == null)
            {
                kvs = create(accCtx, kvsName);
                kvsRepo.put(apiCtx.get(), kvsName, kvs);
            }

            Props props = kvs.getProps(accCtx);
            for (Entry<String, String> entry : modProps.entrySet())
            {
                props.setProp(entry.getKey(), entry.getValue());
            }
            for (String key : deletePropKeys)
            {
                props.removeProp(key);
            }

            ctrlTransactionHelper.commit();

            apiCallRc.addEntry(
                "Successfully updated properties",
                ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_MOD | ApiConsts.MODIFIED
            );
        }
        catch (SQLException exc)
        {
            apiCallRc.addEntry(
                ResponseUtils.getSqlMsg("Persisting properties in instancename '" + kvsNameStr + "'"),
                ApiConsts.FAIL_SQL
            );
        }
        catch (InvalidKeyException exc)
        {
            apiCallRc.addEntry(
                "Invalid key: '" + exc.invalidKey + "'",
                ApiConsts.FAIL_INVLD_PROP
            );
        }
        catch (InvalidValueException exc)
        {
            apiCallRc.addEntry(
                "Invalid value: '" + exc.value + "' for key: '" + exc.key + "'",
                ApiConsts.FAIL_INVLD_PROP
            );
        }
        catch (InvalidNameException exc)
        {
            apiCallRc.addEntry(
                "Invalid KeyValueStore name used",
                ApiConsts.FAIL_INVLD_KVS_NAME
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

    private KeyValueStoreData create(AccessContext accCtx, KeyValueStoreName kvsName)
    {
        KeyValueStoreData kvs;
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
                "An accessDeniedException occured during creation of a KSV (KeyValueStore). That means the " +
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
                    .build(),
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }

        return kvs;
    }

    public Map<String, String> listProps(String kvsNameStr)
    {
        Map<String, String> retMap;
        try
        {
            KeyValueStoreName kvsName = new KeyValueStoreName(kvsNameStr);

            AccessContext accCtx = peerAccCtx.get();
            KeyValueStoreData kvs = kvsRepo.get(accCtx, kvsName);

            Props props = kvs.getProps(accCtx);
            retMap = Collections.unmodifiableMap(props.map());
        }
        catch (Exception exc)
        {
            exc.printStackTrace();
            retMap = Collections.emptyMap();
        }
        return retMap;
    }
}
