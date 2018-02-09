package com.linbit.linstor.core;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer.Builder;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import java.sql.SQLException;
import java.util.regex.Matcher;

public class CtrlConfApiCallHandler
{
    private final ApiCtrlAccessors apiCtrlAccessors;
    private final CtrlClientSerializer ctrlClientcomSrzl;

    public CtrlConfApiCallHandler(ApiCtrlAccessors apiCtrlAccessorsRef, CtrlClientSerializer ctrlClientcomSrzlRef)
    {
        apiCtrlAccessors = apiCtrlAccessorsRef;
        ctrlClientcomSrzl = ctrlClientcomSrzlRef;
    }

    public ApiCallRc setProp(AccessContext accCtx, String key, String namespace, String value)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            apiCtrlAccessors.getCtrlConfProtection().requireAccess(accCtx, AccessType.CHANGE);

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
                    setTcpPort(accCtx, key, namespace, value, apiCallRc);
                    break;
                case ApiConsts.KEY_MINOR_NR_RANGE:
                    setMinorNr(accCtx, key, namespace, value, apiCallRc);
                    break;
                // TODO: check for other properties
                default:
                    // ignore - for now
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
            apiCtrlAccessors.getErrorReporter().reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    public byte[] listProps(AccessContext accCtx, int msgId)
    {
        byte[] data = null;
        try
        {
            apiCtrlAccessors.getCtrlConfProtection().requireAccess(accCtx, AccessType.VIEW);
            Props conf = apiCtrlAccessors.getCtrlConf();
            Builder builder = ctrlClientcomSrzl.builder(ApiConsts.API_LST_CFG_VAL, msgId);
            builder.ctrlCfgProps(conf.map());

            data = builder.build();
        }
        catch (Exception exc)
        {
            // the list will be empty
        }
        return data;
    }

    public ApiCallRc deleteProp(AccessContext accCtx, String key, String namespace)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            apiCtrlAccessors.getCtrlConfProtection().requireAccess(accCtx, AccessType.CHANGE);

            String fullKey;
            if (namespace != null && !"".equals(namespace.trim()))
            {
                fullKey = namespace + "/" + key;
            }
            else
            {
                fullKey = key;
            }
            String oldValue = apiCtrlAccessors.getCtrlConf().removeProp(key, namespace);

            if (oldValue != null)
            {
                switch (fullKey)
                {
                    case ApiConsts.KEY_TCP_PORT_RANGE:
                        apiCtrlAccessors.reloadTcpPortRange();
                        break;
                    case ApiConsts.KEY_MINOR_NR_RANGE:
                        apiCtrlAccessors.reloadMinorNrRange();
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
            apiCtrlAccessors.getErrorReporter().reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return apiCallRc;
    }

    private void setTcpPort(
        AccessContext accCtx,
        String key,
        String namespace,
        String value,
        ApiCallRcImpl apiCallRc
    )
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException
    {
        Props ctrlCfg = apiCtrlAccessors.getCtrlConf();

        Matcher matcher = Controller.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidTcpPort(accCtx, matcher.group("min"), apiCallRc) &&
                isValidTcpPort(accCtx, matcher.group("max"), apiCallRc)
            )
            {
                ctrlCfg.setProp(key, value, namespace);
                apiCtrlAccessors.reloadTcpPortRange();

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

    private boolean isValidTcpPort(AccessContext accCtx, String strTcpPort, ApiCallRcImpl apiCallRc)
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
            apiCtrlAccessors.getErrorReporter().reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return validTcpPortNr;
    }


    private void setMinorNr(
        AccessContext accCtx,
        String key,
        String namespace,
        String value,
        ApiCallRcImpl apiCallRc
    )
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        Props ctrlCfg = apiCtrlAccessors.getCtrlConf();

        Matcher matcher = Controller.RANGE_PATTERN.matcher(value);
        if (matcher.find())
        {
            if (
                isValidMinorNr(accCtx, matcher.group("min"), apiCallRc) &&
                isValidMinorNr(accCtx, matcher.group("max"), apiCallRc)
            )
            {
                ctrlCfg.setProp(key, value, namespace);
                apiCtrlAccessors.reloadMinorNrRange();

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

    private boolean isValidMinorNr(AccessContext accCtx, String strMinorNr, ApiCallRcImpl apiCallRc)
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
            apiCtrlAccessors.getErrorReporter().reportError(
                exc,
                accCtx,
                null,
                errorMsg
            );
        }
        return isValid;
    }
}
