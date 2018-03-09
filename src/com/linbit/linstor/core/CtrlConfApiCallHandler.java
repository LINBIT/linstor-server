package com.linbit.linstor.core;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer.Builder;
import com.linbit.linstor.logging.ErrorReporter;
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

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.regex.Matcher;

public class CtrlConfApiCallHandler
{
    private ErrorReporter errorReporter;
    private final CtrlClientSerializer ctrlClientcomSrzl;
    private final ObjectProtection ctrlConfProt;
    private final Props ctrlConf;
    private final DynamicNumberPool tcpPortPool;
    private final DynamicNumberPool minorNrPool;
    private final AccessContext accCtx;

    @Inject
    public CtrlConfApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlClientSerializer ctrlClientcomSrzlRef,
        @Named(ControllerSecurityModule.CTRL_CONF_PROT) ObjectProtection ctrlConfProtRef,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(NumberPoolModule.TCP_PORT_POOL) DynamicNumberPool tcpPortPoolRef,
        @Named(NumberPoolModule.MINOR_NUMBER_POOL) DynamicNumberPool minorNrPoolRef,
        @PeerContext AccessContext accCtxRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlClientcomSrzl = ctrlClientcomSrzlRef;
        ctrlConfProt = ctrlConfProtRef;
        ctrlConf = ctrlConfRef;
        tcpPortPool = tcpPortPoolRef;
        minorNrPool = minorNrPoolRef;
        accCtx = accCtxRef;
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
                case ApiConsts.KEY_TCP_PORT_RANGE:
                    setTcpPort(key, namespace, value, apiCallRc);
                    break;
                case ApiConsts.KEY_MINOR_NR_RANGE:
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
                    case ApiConsts.KEY_TCP_PORT_RANGE:
                        tcpPortPool.reloadRange();
                        break;
                    case ApiConsts.KEY_MINOR_NR_RANGE:
                        minorNrPool.reloadRange();
                        break;
                    // TODO: check for other properties
                    default:
                        // ignore - for now
                }
            }
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
