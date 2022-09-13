package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.StorageException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SEDUtils
{
    private static final boolean LOG_SED_COMMANDS = false;

    public static ApiCallRc initializeSED(
        final ExtCmd extCmd,
        final ErrorReporter errorReporter,
        final ApiCallRcImpl apiCallRc,
        final String deviceName,
        final String password
    )
    {
        try
        {
            List<String> cmd = new ArrayList<>();
            cmd.add("sedutil-cli");
            cmd.add("--initialsetup");
            cmd.add(password);
            cmd.add(deviceName);

            final String failMsg = "Failed to initialized SED encryption on drive: " + deviceName;
            extCmd.logExecution(LOG_SED_COMMANDS);
            errorReporter.logInfo("Initialize SED %s", deviceName);
            Commands.genericExecutor(
                extCmd,
                cmd.toArray(new String[0]),
                failMsg,
                failMsg
            );

            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                String.format("initialized SED encryption on drive: %s", deviceName)));
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return apiCallRc;
    }

    public static void unlockSED(
        final ExtCmd extCmd,
        final ErrorReporter errorReporter,
        final String deviceName,
        final String password)
    {
        try
        {
            errorReporter.logInfo("Unlock SED %s", deviceName);
            extCmd.logExecution(LOG_SED_COMMANDS);
            // set locking range to rw
            List<String> cmd = new ArrayList<>();
            cmd.add("sedutil-cli");
            cmd.add("--setlockingrange");
            cmd.add("0");
            cmd.add("rw");
            cmd.add(password);
            cmd.add(deviceName);

            final String failMsg = "Failed to setlockingrange to RW on SED: " + deviceName;
            Commands.genericExecutor(
                extCmd,
                cmd.toArray(new String[0]),
                failMsg,
                failMsg
            );


            List<String> cmdMbrDone = new ArrayList<>();
            cmdMbrDone.add("sedutil-cli");
            cmdMbrDone.add("--setmbrdone");
            cmdMbrDone.add("on");
            cmdMbrDone.add(password);
            cmdMbrDone.add(deviceName);

            final String failMsgMbrDone = "Failed to setmbrdone on SED: " + deviceName;
            Commands.genericExecutor(
                extCmd,
                cmdMbrDone.toArray(new String[0]),
                failMsgMbrDone,
                failMsgMbrDone
            );

            errorReporter.logInfo("Unlocked SED: %s", deviceName);
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
        }
    }

    public static void changeSEDPassword(
        final ExtCmd extCmd,
        final ErrorReporter errorReporter,
        final String deviceName,
        final String password,
        final String newPassword)
    {
        try
        {
            errorReporter.logInfo("Changing SED passwords for %s", deviceName);
            extCmd.logExecution(LOG_SED_COMMANDS);
            // set locking range to rw
            List<String> cmd = new ArrayList<>();
            cmd.add("sedutil-cli");
            cmd.add("--setSIDPassword");
            cmd.add(password);
            cmd.add(newPassword);
            cmd.add(deviceName);

            final String failMsg = "Failed to change SID password on SED: " + deviceName;
            Commands.genericExecutor(
                extCmd,
                cmd.toArray(new String[0]),
                failMsg,
                failMsg
            );


            List<String> cmdMbrDone = new ArrayList<>();
            cmdMbrDone.add("sedutil-cli");
            cmdMbrDone.add("--setAdmin1Pwd");
            cmdMbrDone.add(password);
            cmdMbrDone.add(newPassword);
            cmdMbrDone.add(deviceName);

            final String failMsgAdminPass = "Failed to change Admin1 password on SED: " + deviceName;
            Commands.genericExecutor(
                extCmd,
                cmdMbrDone.toArray(new String[0]),
                failMsgAdminPass,
                failMsgAdminPass
            );

            errorReporter.logInfo("Changed SED passwords for %s", deviceName);
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
        }
    }

    /**
     * Returns a map of SED device names as keys and encrypted passwords as values.
     * @param sedProps
     * @return
     */
    public static Map<String, String> drivePasswordMap(Map<String, String> sedProps)
    {
        final HashMap<String, String> driveMap = new HashMap<>();
        for (final String fullKey : sedProps.keySet())
        {
            if (fullKey.startsWith(ApiConsts.NAMESPC_SED + Props.PATH_SEPARATOR))
            {
                final String drive = fullKey.substring(ApiConsts.NAMESPC_SED.length());
                final String sedEncPassword = sedProps.get(fullKey);
                driveMap.put(drive, sedEncPassword);
            }
        }
        return driveMap;
    }
}
