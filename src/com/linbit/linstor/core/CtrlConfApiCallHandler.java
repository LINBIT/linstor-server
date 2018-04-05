package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.crypto.SymmetricKeyCipher;
import com.linbit.crypto.SymmetricKeyCipher.CipherStrength;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.Node;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer.Builder;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Base64;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.inject.Provider;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;

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
    private final CtrlClientSerializer ctrlClientcomSrzl;
    private final ObjectProtection ctrlConfProt;
    private final Props ctrlConf;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorNrPool;
    private final AccessContext accCtx;
    private final CtrlSecurityObjects ctrlSecObj;
    private final Provider<Peer> peerProvider;
    private final Provider<TransactionMgr> transMgrProvider;

    private final CtrlStltSerializer ctrlStltSrzl;
    private final NodesMap nodesMap;
    private final AccessContext apiCtx;

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
        CtrlClientSerializer ctrlClientcomSrzlRef,
        @Named(ControllerSecurityModule.CTRL_CONF_PROT) ObjectProtection ctrlConfProtRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @PeerContext AccessContext accCtxRef,
        CtrlSecurityObjects ctrlSecObjRef,
        Provider<Peer> peerProviderRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.NodesMap nodesMapRef,
        CtrlStltSerializer ctrlStltSrzlRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlClientcomSrzl = ctrlClientcomSrzlRef;
        ctrlConfProt = ctrlConfProtRef;
        ctrlConf = ctrlConfRef;
        tcpPortPool = tcpPortPoolRef;
        minorNrPool = minorNrPoolRef;
        accCtx = accCtxRef;
        ctrlSecObj = ctrlSecObjRef;
        peerProvider = peerProviderRef;
        transMgrProvider = transMgrProviderRef;

        apiCtx = apiCtxRef;
        nodesMap = nodesMapRef;
        ctrlStltSrzl = ctrlStltSrzlRef;
    }

    public ApiCallRc setProp(String key, String namespace, String value)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            ctrlConfProt.requireAccess(accCtx, AccessType.CHANGE);

            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            switch (fullKey)
            {
                case ApiConsts.KEY_TCP_PORT_AUTO_RANGE:
                    setTcpPort(key, namespace, value, apiCallRc);
                    break;
                case ApiConsts.KEY_MINOR_NR_AUTO_RANGE:
                    setMinorNr(key, namespace, value, apiCallRc);
                    break;
                // TODO: check for other properties
                default:
                {
                    apiCallRc.addEntry(
                        String.format("Setting property '%s' is currently not supported.", fullKey),
                        ApiConsts.FAIL_INVLD_PROP
                    );
                }
                break;
            }

            transMgrProvider.get().commit();
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = AbsApiCallHandler.getAccDeniedMsg(
                    accCtx,
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
            if (exc instanceof SQLException)
            {
                errorMsg = AbsApiCallHandler.getSqlMsg(
                    "Persisting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "' with value '" + value + "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An unknown error occured while setting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "' with value '" + value + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc);
            errorReporter.reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public byte[] listProps(int msgId)
    {
        byte[] data = null;
        try
        {
            ctrlConfProt.requireAccess(accCtx, AccessType.VIEW);
            Builder builder = ctrlClientcomSrzl.builder(ApiConsts.API_LST_CFG_VAL, msgId);
            builder.ctrlCfgProps(ctrlConf.map());

            data = builder.build();
        }
        catch (Exception exc)
        {
            // the list will be empty
        }
        return data;
    }

    public ApiCallRc deleteProp(String key, String namespace)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            ctrlConfProt.requireAccess(accCtx, AccessType.CHANGE);

            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            String oldValue = ctrlConf.removeProp(key, namespace);

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
        }
        catch (Exception exc)
        {
            String errorMsg;
            long rc;
            if (exc instanceof AccessDeniedException)
            {
                errorMsg = AbsApiCallHandler.getAccDeniedMsg(
                    accCtx,
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
            if (exc instanceof SQLException)
            {
                errorMsg = AbsApiCallHandler.getSqlMsg(
                    "Deleting controller config prop with key '" + key + "' in namespace '" + namespace +
                    "'."
                );
                rc = ApiConsts.FAIL_SQL;
            }
            else
            {
                errorMsg = "An unknown error occured while deleting controller config prop with key '" +
                    key + "' in namespace '" + namespace + "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(errorMsg, rc);
            errorReporter.reportError(
                exc,
                accCtx,
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
            ctrlConfProt.requireAccess(accCtx, AccessType.VIEW);

            Props namespace = ctrlConf.getNamespace(NAMESPACE_ENCRYPTED).orElse(null);
            if (namespace == null || namespace.isEmpty())
            {
                AbsApiCallHandler.reportStatic(
                    null,
                    NAMESPACE_ENCRYPTED + " namespace is empty",
                    null, // cause
                    null, // details
                    null, // correction
                    ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null, // objRefs
                    null, // variables
                    apiCallRc,
                    errorReporter,
                    accCtx,
                    peerProvider.get()
                );
            }
            else
            {
                byte[] decryptedMasterKey = getDecryptedMasterKey(namespace, passphrase, apiCallRc);
                if (decryptedMasterKey != null)
                {
                    setCryptKey(decryptedMasterKey);

                    AbsApiCallHandler.reportSuccessStatic(
                        "Passphrase accepted",
                        null,
                        ApiConsts.MASK_CTRL_CONF | ApiConsts.PASSPHRASE_ACCEPTED,
                        apiCallRc,
                        null,
                        null,
                        errorReporter
                    );
                } // else: report added in getDecryptedMasterKey method
            }
        }
        catch (AccessDeniedException exc)
        {
            AbsApiCallHandler.addAnswerStatic(
                AbsApiCallHandler.getAccDeniedMsg(accCtx, "view the controller properties"),
                null, // cause
                null, // details
                null, // correction
                ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objRefs
                null, // variables
                apiCallRc
            );
        }
        catch (InvalidKeyException exc)
        {
            AbsApiCallHandler.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    exc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        catch (LinStorException exc)
        {
            AbsApiCallHandler.reportStatic(
                exc,
                "Unknown error occured while validating the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
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
            ctrlConfProt.requireAccess(accCtx, AccessType.CHANGE);

            Props namespace = ctrlConf.getNamespace(NAMESPACE_ENCRYPTED).orElse(null);

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
                    AbsApiCallHandler.reportSuccessStatic(
                         "Crypt passphrase created.",
                         null, // details
                         mask | ApiConsts.CREATED,
                         apiCallRc,
                         null, // objectRefs
                         null, // variables
                         errorReporter
                    );
                }
                else
                {
                    AbsApiCallHandler.addAnswerStatic(
                        "Coult not create new crypt passphrase as it already exists",
                        "A passphrase was already defined",
                        null,
                        "Use the crypt-modify-passphrase command instead of crypt-create-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        new HashMap<>(),
                        apiCallRc
                    );
                }
            }
            else
            {
                mask |= ApiConsts.MASK_MOD;
                if (namespace == null || namespace.isEmpty())
                {
                    AbsApiCallHandler.addAnswerStatic(
                        "Coult not modify crypt passphrase as it does not exist",
                        "No passphrase was defined yet",
                        null,
                        "Use the crypt-create-passphrase command instead of crypt-modify-passphrase",
                        mask | ApiConsts.FAIL_EXISTS_CRYPT_PASSPHRASE,
                        new HashMap<>(),
                        new HashMap<>(),
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
                        AbsApiCallHandler.reportSuccessStatic(
                            "Crypt passphrase updated",
                            null, // details
                            mask | ApiConsts.MODIFIED,
                            apiCallRc,
                            null, // objectRefs
                            null, // variables
                            errorReporter
                       );
                    } // else: error was already reported in getDecryptedMasterKey method
                }
            }

        }
        catch (InvalidKeyException invalidNameExc)
        {
            AbsApiCallHandler.reportStatic(
                new ImplementationError(
                    "Hardcoded namespace or property key invalid",
                    invalidNameExc
                ),
                "Hardcoded namespace or property key invalid",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            AbsApiCallHandler.reportStatic(
                accDeniedExc,
                AbsApiCallHandler.getAccDeniedMsg(accCtx, "access the controller properties"),
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG,
                null, // objects
                null, // variables
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        catch (InvalidValueException exc)
        {
            AbsApiCallHandler.reportStatic(
                new ImplementationError(exc),
                "Generated key could not be stored as property",
                ApiConsts.FAIL_IMPL_ERROR,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        catch (SQLException exc)
        {
            AbsApiCallHandler.reportStatic(
                exc,
                AbsApiCallHandler.getSqlMsg("storing the generated and encrypted master key"),
                ApiConsts.FAIL_SQL,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        catch (LinStorException exc)
        {
            AbsApiCallHandler.reportStatic(
                exc,
                "An unknown exception occured while setting the passphrase",
                ApiConsts.FAIL_UNKNOWN_ERROR,
                null,
                null,
                apiCallRc,
                errorReporter,
                accCtx,
                peerProvider.get()
            );
        }
        return apiCallRc;
    }

    private void setPassphraseImpl(String newPassphrase, byte[] masterKey)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException, LinStorException
    {
        // store the hash of the masterkey in the database
        sha512.reset();
        byte[] hash = sha512.digest(masterKey);
        ctrlConf.setProp(KEY_CRYPT_HASH, Base64.encode(hash), NAMESPACE_ENCRYPTED);

        // next, encrypt the masterKey with the newPassphrase and store it in the database
        byte[] salt = SecretGenerator.generateSecret(MASTER_KEY_SALT_BYTES);
        SymmetricKeyCipher cipher = SymmetricKeyCipher.getInstanceWithPassword(
            salt,
            newPassphrase.getBytes(StandardCharsets.UTF_8),
            CipherStrength.KEY_LENGTH_128 // TODO if MASTER_KEY_BYTES is configurable, this also has to be configurable
        );

        byte[] encryptedMasterKey = cipher.encrypt(masterKey);
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
            AbsApiCallHandler.addAnswerStatic(
                "Could not restore crypt passphrase as one of the following properties is not set:\n" +
                    "'" + KEY_CRYPT_HASH + "', '" + KEY_CRYPT_KEY + "', '" + KEY_PASSPHRASE_SALT + "'",
                "This is either an implementation error or a user has manually removed one of the " +
                    "mentioned protperties.",
                null, // details
                null, // correction
                ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                null, // objectRefs
                null, // variables
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
            // TODO if MASTER_KEY_BYTES is configurable, the CipherStrength also has to be configurable

            byte[] decryptedMasterKey = ciper.decrypt(encryptedMasterKey);

            sha512.reset();
            byte[] hashedMasterKey = sha512.digest(decryptedMasterKey);

            if (Arrays.equals(hashedMasterKey, Base64.decode(masterHashStr)))
            {
                ret = decryptedMasterKey;
            }
            else
            {
                AbsApiCallHandler.addAnswerStatic(
                    "Could not restore master passphrase as the given old passphrase was incorrect",
                    "Wrong passphrase", // cause
                    null, // details
                    "Enter the correct passphrase", // correction
                    ApiConsts.MASK_MOD | ApiConsts.MASK_CTRL_CONF | ApiConsts.FAIL_MISSING_PROPS,
                    null, // objectRefs
                    null, // variables
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
                    ctrlStltSrzl.builder(InternalApiConsts.API_CRYPT_KEY, 0)
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
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException
    {
        Props ctrlCfg = ctrlConf;

        Matcher matcher = Controller.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidTcpPort(matcher.group("min"), apiCallRc) &&
                isValidTcpPort(matcher.group("max"), apiCallRc)
            )
            {
                ctrlCfg.setProp(key, value, namespace);
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
                Controller.RANGE_PATTERN.pattern() + "'";
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
                errorMsg = "An unknown exception occured verifying the given TCP port '" + strTcpPort +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                accCtx,
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
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        Props ctrlCfg = ctrlConf;

        Matcher matcher = Controller.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidMinorNr(matcher.group("min"), apiCallRc) &&
                isValidMinorNr(matcher.group("max"), apiCallRc)
            )
            {
                ctrlCfg.setProp(key, value, namespace);
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
                Controller.RANGE_PATTERN.pattern() + "'";
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
                errorMsg = "An unknown exception occured verifying the given minor number'" + strMinorNr +
                    "'.";
                rc = ApiConsts.FAIL_UNKNOWN_ERROR;
            }

            apiCallRc.addEntry(
                errorMsg,
                rc
            );
            errorReporter.reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return isValid;
    }
}
