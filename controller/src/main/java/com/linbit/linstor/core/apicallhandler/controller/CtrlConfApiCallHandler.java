package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.LengthPadding;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.crypto.SymmetricKeyCipher.CipherStrength;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.SecretGenerator;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import com.google.inject.Provider;

@Singleton
public class CtrlConfApiCallHandler
{
    private static final String NAMESPACE_ENCRYPTED = "encrypted";
    private static final String KEY_CRYPT_HASH = "masterhash";
    private static final String KEY_CRYPT_KEY = "masterkey";
    private static final String KEY_PASSPHRASE_SALT = "passphrasesalt";
    private static final int MASTER_KEY_BYTES = 16; // TODO make configurable
    private static final int MASTER_KEY_SALT_BYTES = 16; // TODO make configurable

    private static MessageDigest sha512;

    private ErrorReporter errorReporter;
    private final SystemConfRepository systemConfRepository;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorNrPool;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlSecurityObjects ctrlSecObj;
    private final Provider<Peer> peerProvider;
    private final Provider<TransactionMgr> transMgrProvider;

    private final CtrlStltSerializer ctrlStltSrzl;
    private final NodesMap nodesMap;
    private final AccessContext apiCtx;
    private final WhitelistProps whitelistProps;
    private final LengthPadding cryptoLenPad;

    static
    {
        try
        {
            sha512 = MessageDigest.getInstance("SHA-512");
        }
        catch (NoSuchAlgorithmException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    @Inject
    public CtrlConfApiCallHandler(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlSecurityObjects ctrlSecObjRef,
        Provider<Peer> peerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.NodesMap nodesMapRef,
        CtrlStltSerializer ctrlStltSrzlRef,
        WhitelistProps whitelistPropsRef,
        LengthPadding cryptoLenPadRef
    )
    {
        errorReporter = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        tcpPortPool = tcpPortPoolRef;
        minorNrPool = minorNrPoolRef;
        peerAccCtx = peerAccCtxRef;
        ctrlSecObj = ctrlSecObjRef;
        peerProvider = peerProviderRef;
        transMgrProvider = transMgrProviderRef;

        apiCtx = apiCtxRef;
        nodesMap = nodesMapRef;
        ctrlStltSrzl = ctrlStltSrzlRef;
        whitelistProps = whitelistPropsRef;
        cryptoLenPad = cryptoLenPadRef;
    }

    private void updateSatelliteConf() throws AccessDeniedException
    {
        for (Node nodeToContact : nodesMap.values())
        {
            Peer satellitePeer = nodeToContact.getPeer(peerAccCtx.get());

            if (satellitePeer.isConnected() && !satellitePeer.hasFullSyncFailed())
            {
                byte[] changedMessage = ctrlStltSrzl
                    .onewayBuilder(InternalApiConsts.API_CHANGED_CONTROLLER)
                    .build();

                satellitePeer.sendMessage(changedMessage);
            }
        }
    }

    public ApiCallRc modifyCtrl(
        Map<String, String> overridePropsRef,
        Set<String> deletePropKeysRef,
        Set<String> deletePropNamespacesRef
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        for (Entry<String, String> overrideProp : overridePropsRef.entrySet())
        {
            apiCallRc.addEntries(setProp(overrideProp.getKey(), null, overrideProp.getValue()));
        }
        for (String deletePropKey : deletePropKeysRef)
        {
            apiCallRc.addEntries(deleteProp(deletePropKey, null));
        }
        for (String deleteNamespace : deletePropNamespacesRef)
        {
            // we should not simply "drop" the namespace here, as we might have special cleanup logic
            // for some of the deleted keys.
            apiCallRc.addEntries(deleteNamespace(deleteNamespace));
        }
        return apiCallRc;
    }


    private ApiCallRcImpl deleteNamespace(String deleteNamespaceRef)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            Optional<Props> optNamespace = systemConfRepository.getCtrlConfForChange(peerAccCtx.get()).getNamespace(
                deleteNamespaceRef
            );
            if (optNamespace.isPresent())
            {
                Iterator<String> keysIterator = optNamespace.get().keysIterator();
                while (keysIterator.hasNext())
                {
                    apiCallRc.addEntries(deleteProp(keysIterator.next(), deleteNamespaceRef));
                }

                Iterator<String> iterateNamespaces = optNamespace.get().iterateNamespaces();
                while (iterateNamespaces.hasNext())
                {
                    apiCallRc.addEntries(deleteNamespace(deleteNamespaceRef + "/" + iterateNamespaces.next()));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            String errorMsg = ResponseUtils.getAccDeniedMsg(
                peerAccCtx.get(),
                "set a controller config property"
            );
            apiCallRc.addEntry(
                errorMsg,
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public ApiCallRc setProp(String key, String namespace, String value)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            List<String> ignoredKeys = new ArrayList<>();
            ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");
            if (whitelistProps.isAllowed(LinStorObject.CONTROLLER, ignoredKeys, fullKey, value, false))
            {
                String normalized = whitelistProps.normalize(LinStorObject.CONTROLLER, fullKey, value);
                if (fullKey.startsWith(ApiConsts.NAMESPC_REST + '/'))
                {
                    systemConfRepository.setCtrlProp(peerAccCtx.get(), key, normalized, namespace);
                }
                else
                {
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            setTcpPort(key, namespace, normalized, apiCallRc);
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            setMinorNr(key, namespace, normalized, apiCallRc);
                            break;
                        case ApiConsts.KEY_SEARCH_DOMAIN: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER: // fall-through
                        case ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_QUORUM:
                            systemConfRepository.setCtrlProp(peerAccCtx.get(), key, normalized, namespace);
                            break;
                        case ApiConsts.KEY_EXT_CMD_WAIT_TO:
                            try
                            {
                                long timeout = Long.parseLong(value);
                                if (timeout < 0)
                                {
                                    throw new ApiRcException(
                                        ApiCallRcImpl.simpleEntry(
                                            ApiConsts.FAIL_INVLD_PROP,
                                            "The " + ApiConsts.KEY_EXT_CMD_WAIT_TO + " must not be negative"
                                        )
                                    );

                                }
                                ChildProcessHandler.dfltWaitTimeout = timeout;
                            }
                            catch (NumberFormatException exc)
                            {
                                throw new ApiRcException(
                                    ApiCallRcImpl.simpleEntry(
                                        ApiConsts.FAIL_INVLD_PROP,
                                        "The " + ApiConsts.KEY_EXT_CMD_WAIT_TO + " has to have a numeric value"
                                    ),
                                    exc
                                );
                            }
                        default:
                            systemConfRepository.setStltProp(peerAccCtx.get(), fullKey, normalized);
                            break;
                    }
                }
                transMgrProvider.get().commit();

                updateSatelliteConf();

                apiCallRc.addEntry(
                    "Successfully set property '" + fullKey + "' to value '" + normalized + "'",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                if (whitelistProps.isKeyKnown(LinStorObject.CONTROLLER, fullKey))
                {
                    entry.setMessage("The value '" + value + "' is not valid.");
                    entry.setDetails("The value must match: " +
                        whitelistProps.getRuleValue(LinStorObject.CONTROLLER, fullKey)
                    );
                }
                else
                {
                    entry.setMessage("The key '" + fullKey + "' is not whitelisted");
                }
                entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
                apiCallRc.addEntry(entry);
            }
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "set a controller config property"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_CTRL_CFG;
            }
            else
            if (exc instanceof InvalidKeyException)
            {
                errorMsg = "Invalid key: " + ((InvalidKeyException) exc).invalidKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            if (exc instanceof InvalidValueException)
            {
                errorMsg = "Invalid value: " + value;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            if (exc instanceof DatabaseException)
            {
                errorMsg = ResponseUtils.getSqlMsg(
                    "Persisting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "' with value '" + value + "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An unknown error occurred while setting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "' with value '" + value + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT);
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public Map<String, String> listProps()
    {
        Map<String, String> mergedMap = new TreeMap<>();
        try
        {
            mergedMap.putAll(systemConfRepository.getCtrlConfForView(peerAccCtx.get()).map());
            mergedMap.putAll(systemConfRepository.getStltConfForView(peerAccCtx.get()).map());
        }
        catch (Exception ignored)
        {
            // empty list
        }
        return mergedMap;
    }

    public ApiCallRc deleteProp(String key, String namespace)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            boolean isPropWhitelisted = whitelistProps.isAllowed(
                LinStorObject.CONTROLLER,
                Collections.singletonList(ApiConsts.NAMESPC_AUXILIARY + "/"),
                fullKey,
                null,
                false
            );
            if (isPropWhitelisted)
            {
                String oldValue = systemConfRepository.removeCtrlProp(peerAccCtx.get(), key, namespace);
                systemConfRepository.removeStltProp(peerAccCtx.get(), key, namespace);

                if (oldValue != null)
                {
                    switch (fullKey)
                    {
                        case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                            tcpPortPool.reloadRange();
                            break;
                        case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                            minorNrPool.reloadRange();
                            break;
                        // TODO: check for other properties
                        default:
                            // ignore - for now
                    }
                }

                transMgrProvider.get().commit();

                updateSatelliteConf();

                apiCallRc.addEntry(
                    "Successfully deleted property '" + fullKey + "'",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_DEL | ApiConsts.DELETED
                );
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setMessage("The key '" + fullKey + "' is not whitelisted");
                entry.setReturnCode(ApiConsts.FAIL_INVLD_PROP | ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_DEL);
                apiCallRc.addEntry(entry);
            }
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = ResponseUtils.getAccDeniedMsg(
                    peerAccCtx.get(),
                    "delete a controller config property"
                );
                rc = ApiConsts.FAIL_ACC_DENIED_CTRL_CFG;
            }
            else
            if (exc instanceof InvalidKeyException)
            {
                errorMsg = "Invalid key: " + ((InvalidKeyException) exc).invalidKey;
                rc = ApiConsts.FAIL_INVLD_PROP;
            }
            else
            if (exc instanceof DatabaseException)
            {
                errorMsg = ResponseUtils.getSqlMsg(
                    "Deleting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An unknown error occurred while deleting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc);
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public ApiCallRc enterPassphrase(String passphrase)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            Props namespace = systemConfRepository.getCtrlConfForView(peerAccCtx.get())
                .getNamespace(NAMESPACE_ENCRYPTED).orElse(null);
            if (namespace == null || namespace.isEmpty())
            {
                ResponseUtils.reportStatic(
                    null,
                    NAMESPACE_ENCRYPTED + " namespace is empty",
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null, // objRefs
                    apiCallRc,
                    errorReporter,
                    peerAccCtx.get(),
                    peerProvider.get()
                );
            }
            else
            {
                byte[] decryptedMasterKey = getDecryptedMasterKey(namespace, passphrase, apiCallRc);
                if (decryptedMasterKey != null)
                {
                    setCryptKey(decryptedMasterKey);

                    ResponseUtils.reportSuccessStatic(
                        "Passphrase accepted",
                        null,
                        ApiConsts.MASK_CTRL_CONF | ApiConsts.PASSPHRASE_ACCEPTED,
                        apiCallRc,
                        null,
                        errorReporter
                    );
                } // else: report added in getDecryptedMasterKey method
            }
        }
        catch (AccessDeniedException exc)
        {
            ResponseUtils.addAnswerStatic(
                ResponseUtils.getAccDeniedMsg(peerAccCtx.get(), "view the controller properties"),
                null, // cause
                null, // details
                null, // correction
                ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objRefs
                null, // errorId
                apiCallRc
            );
        }
        catch (InvalidKeyException exc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    exc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "Unknown error occurred while validating the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }

        return apiCallRc;
    }

    public ApiCallRc setPassphrase(String newPassphrase, String oldPassphrase)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        long mask = ApiConsts.MASK_CTRL_CONF;
        try
        {
            Props namespace = systemConfRepository.getCtrlConfForChange(peerAccCtx.get())
                .getNamespace(NAMESPACE_ENCRYPTED).orElse(null);

            if (oldPassphrase == null)
            {
                mask |= ApiConsts.MASK_CRT;
                if (namespace == null || namespace.getProp(KEY_CRYPT_KEY) == null)
                {
                    // no oldPassphrase and empty namespace means that
                    // this is the initial passphrase
                    byte[] masterKey = SecretGenerator.generateSecret(MASTER_KEY_BYTES);
                    setPassphraseImpl(
                        newPassphrase,
                        masterKey
                    );
                    setCryptKey(masterKey);
                    ResponseUtils.reportSuccessStatic(
                         "Crypt passphrase created.",
                         null, // details
                         mask | ApiConsts.CREATED,
                         apiCallRc,
                         null, // objectRefs
                         errorReporter
                    );
                }
                else
                {
                    ResponseUtils.addAnswerStatic(
                        "Coult not create new crypt passphrase as it already exists",
                        "A passphrase was already defined",
                        null,
                        "Use the crypt-modify-passphrase command instead of crypt-create-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
                        apiCallRc
                    );
                }
            }
            else
            {
                mask |= ApiConsts.MASK_MOD;
                if (namespace == null || namespace.isEmpty())
                {
                    ResponseUtils.addAnswerStatic(
                        "Coult not modify crypt passphrase as it does not exist",
                        "No passphrase was defined yet",
                        null,
                        "Use the crypt-create-passphrase command instead of crypt-modify-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        null, // errorId
                        apiCallRc
                    );
                }
                else
                {
                    byte[] decryptedMasterKey = getDecryptedMasterKey(
                        namespace,
                        oldPassphrase,
                        apiCallRc
                    );
                    if (decryptedMasterKey != null)
                    {
                        setPassphraseImpl(newPassphrase, decryptedMasterKey);
                        ResponseUtils.reportSuccessStatic(
                            "Crypt passphrase updated",
                            null, // details
                            mask | ApiConsts.MODIFIED,
                            apiCallRc,
                            null, // objectRefs
                            errorReporter
                       );
                    } // else: error was already reported in getDecryptedMasterKey method
                }
            }

        }
        catch (InvalidKeyException invalidNameExc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    invalidNameExc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ResponseUtils.reportStatic(
                accDeniedExc,
                ResponseUtils.getAccDeniedMsg(peerAccCtx.get(), "access the controller properties"),
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objects
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (InvalidValueException exc)
        {
            ResponseUtils.reportStatic(
                new ImplementationError(exc),
                "Generated key could not be stored as property",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (DatabaseException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                ResponseUtils.getSqlMsg("storing the generated and encrypted master key"),
                ApiConsts.FAIL_SQL,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        catch (LinStorException exc)
        {
            ResponseUtils.reportStatic(
                exc,
                "An unknown exception occurred while setting the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                apiCallRc,
                errorReporter,
                peerAccCtx.get(),
                peerProvider.get()
            );
        }
        return apiCallRc;
    }

    boolean passphraseExists() throws AccessDeniedException
    {
        Props namespace = systemConfRepository.getCtrlConfForView(peerAccCtx.get()).getNamespace(NAMESPACE_ENCRYPTED)
            .orElse(null);

        boolean exists = false;
        if (namespace != null)
        {
            String masterHashStr = namespace.getProp(KEY_CRYPT_HASH);
            String encryptedMasterKeyStr = namespace.getProp(KEY_CRYPT_KEY);
            String passphraseSaltStr = namespace.getProp(KEY_PASSPHRASE_SALT);

            exists = masterHashStr != null &&
                encryptedMasterKeyStr != null &&
                passphraseSaltStr != null;
        }
        return exists;
    }

    private void setPassphraseImpl(String newPassphrase, byte[] masterKey)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException, LinStorException
    {
        Props ctrlConf = systemConfRepository.getCtrlConfForChange(peerAccCtx.get());

        // Add length padding to the master key, encrypt with the new passphrase and a generated salt,
        // and store the encrypted key, the salt and a hash of the length padded key in the database
        byte[] salt = SecretGenerator.generateSecret(MASTER_KEY_SALT_BYTES);
        SymmetricKeyCipher cipher = SymmetricKeyCipher.getInstanceWithPassword(
            salt,
            newPassphrase.getBytes(StandardCharsets.UTF_8),
            CipherStrength.KEY_LENGTH_128 // TODO if MASTER_KEY_BYTES is configurable, this also has to be configurable
        );

        byte[] encodedData = cryptoLenPad.conceal(masterKey);
        // Store a hash of the length padded key in the database
        sha512.reset();
        byte[] hash = sha512.digest(encodedData);
        ctrlConf.setProp(KEY_CRYPT_HASH, Base64.encode(hash), NAMESPACE_ENCRYPTED);
        byte[] encryptedMasterKey = cipher.encrypt(encodedData);

        ctrlConf.setProp(KEY_CRYPT_KEY, Base64.encode(encryptedMasterKey), NAMESPACE_ENCRYPTED);
        ctrlConf.setProp(KEY_PASSPHRASE_SALT, Base64.encode(salt), NAMESPACE_ENCRYPTED);

        transMgrProvider.get().commit();
    }

    private byte[] getDecryptedMasterKey(Props namespace, String oldPassphrase, ApiCallRcImpl apiCallRc)
        throws InvalidKeyException, LinStorException
    {
        byte[] ret = null;
        String masterHashStr = namespace.getProp(KEY_CRYPT_HASH);
        String encryptedMasterKeyStr = namespace.getProp(KEY_CRYPT_KEY);
        String passphraseSaltStr = namespace.getProp(KEY_PASSPHRASE_SALT);

        if (
            masterHashStr == null ||
            encryptedMasterKeyStr == null ||
            passphraseSaltStr == null
        )
        {
            ResponseUtils.addAnswerStatic(
                "Could not restore crypt passphrase as one of the following properties is not set:\n" +
                    "'" + KEY_CRYPT_HASH + "', '" + KEY_CRYPT_KEY + "', '" + KEY_PASSPHRASE_SALT + "'",
                "This is either an implementation error or a user has manually removed one of the " +
                    "mentioned protperties.",
                null, // details
                null, // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // errorId
                apiCallRc
            );
        }
        else
        {
            byte[] passphraseSalt = Base64.decode(passphraseSaltStr);
            byte[] encryptedMasterKey = Base64.decode(encryptedMasterKeyStr);

            SymmetricKeyCipher ciper =  SymmetricKeyCipher.getInstanceWithPassword(
                passphraseSalt,
                oldPassphrase.getBytes(StandardCharsets.UTF_8),
                CipherStrength.KEY_LENGTH_128
            );
            // TODO: if MASTER_KEY_BYTES is configurable, the CipherStrength also has to be configurable

            byte[] decryptedData = ciper.decrypt(encryptedMasterKey);

            sha512.reset();
            byte[] hashedMasterKey = sha512.digest(decryptedData);

            if (Arrays.equals(hashedMasterKey, Base64.decode(masterHashStr)))
            {
                ret = cryptoLenPad.retrieve(decryptedData);
            }
            else
            {
                ResponseUtils.addAnswerStatic(
                    "Could not restore master passphrase as the given old passphrase was incorrect",
                    "Wrong passphrase", // cause
                    null, // details
                    "Enter the correct passphrase", // correction
                    ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null, // objectRefs
                    null, // errorId
                    apiCallRc
                );
            }
        }
        return ret;
    }

    private void setCryptKey(byte[] cryptKey)
    {
        ctrlSecObj.setCryptKey(cryptKey);

        for (Node node : nodesMap.values())
        {
            Peer peer;
            try
            {
                peer = node.getPeer(apiCtx);
                peer.sendMessage(
                    ctrlStltSrzl.onewayBuilder(InternalApiConsts.API_CRYPT_KEY)
                        .cryptKey(
                            cryptKey,
                            peer.getFullSyncId(),
                            peer.getNextSerializerId()
                        )
                        .build()
                );
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private void setTcpPort(
        String key,
        String namespace,
        String value,
        ApiCallRcImpl apiCallRc
    )
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException
    {
        Matcher matcher = NumberPoolModule.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidTcpPort(matcher.group("min"), apiCallRc) &&
                isValidTcpPort(matcher.group("max"), apiCallRc)
            )
            {
                systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                tcpPortPool.reloadRange();

                apiCallRc.addEntry(
                    "The TCP port range was successfully updated to: " + value,
                    ApiConsts.MODIFIED
                );
            }
        }
        else
        {
            String errMsg = "The given value '" + value + "' is not a valid range. '" +
                NumberPoolModule.RANGE_PATTERN.pattern() + "'";
            apiCallRc.addEntry(
                errMsg,
                ApiConsts.FAIL_INVLD_TCP_PORT
            );
        }
    }

    private boolean isValidTcpPort(String strTcpPort, ApiCallRcImpl apiCallRc)
    {
        boolean validTcpPortNr = false;
        try
        {
            TcpPortNumber.tcpPortNrCheck(Integer.parseInt(strTcpPort));
            validTcpPortNr = true;
        }
        catch (NumberFormatException | ValueOutOfRangeException exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof NumberFormatException)
            {
                errorMsg = "The given tcp port number is not a valid integer: '" + strTcpPort + "'.";
                rc = ApiConsts.FAIL_INVLD_TCP_PORT;
            }
            else
            if (exc instanceof ValueOutOfRangeException)
            {
                errorMsg = "The given tcp port number is not valid: '" + strTcpPort + "'.";
                rc = ApiConsts.FAIL_INVLD_TCP_PORT;
            }
            else
            {
                errorMsg = "An unknown exception occurred verifying the given TCP port '" + strTcpPort +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return validTcpPortNr;
    }


    private void setMinorNr(
        String key,
        String namespace,
        String value,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, DatabaseException
    {
        Matcher matcher = NumberPoolModule.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidMinorNr(matcher.group("min"), apiCallRc) &&
                isValidMinorNr(matcher.group("max"), apiCallRc)
            )
            {
                systemConfRepository.setCtrlProp(peerAccCtx.get(), key, value, namespace);
                minorNrPool.reloadRange();

                apiCallRc.addEntry(
                    "The Minor range was successfully updated to: " + value,
                    ApiConsts.MODIFIED
                );
            }
        }
        else
        {
            String errMsg = "The given value '" + value + "' is not a valid range. '" +
                NumberPoolModule.RANGE_PATTERN.pattern() + "'";
            apiCallRc.addEntry(
                errMsg,
                ApiConsts.FAIL_INVLD_MINOR_NR
            );
        }
    }

    private boolean isValidMinorNr(String strMinorNr, ApiCallRcImpl apiCallRc)
    {
        boolean isValid = false;
        try
        {
            MinorNumber.minorNrCheck(Integer.parseInt(strMinorNr));
            isValid = true;
        }
        catch (NumberFormatException | ValueOutOfRangeException exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof NumberFormatException)
            {
                errorMsg = "The given minor number is not a valid integer: '" + strMinorNr + "'.";
                rc = ApiConsts.FAIL_INVLD_MINOR_NR;
            }
            else
            if (exc instanceof ValueOutOfRangeException)
            {
                errorMsg = "The given minor number is not valid: '" + strMinorNr + "'.";
                rc = ApiConsts.FAIL_INVLD_MINOR_NR;
            }
            else
            {
                errorMsg = "An unknown exception occurred verifying the given minor number'" + strMinorNr +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                peerAccCtx.get(),
                null,
                errorMsg
            );
        }
        return isValid;
    }
}
