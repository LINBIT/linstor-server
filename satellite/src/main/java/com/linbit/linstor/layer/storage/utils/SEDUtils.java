package com.linbit.linstor.layer.storage.utils;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.utils.Triple;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SEDUtils
{
    private static final boolean LOG_SED_COMMANDS = false;

    public static boolean initializeSED(
        final ExtCmdFactory extCmdFactory,
        final ErrorReporter errorReporter,
        final ApiCallRcImpl apiCallRc,
        final String deviceName,
        final String password
    )
    {
        try
        {
            List<Triple<String, String, String[]>> cmds = new ArrayList<>();
            cmds.add(new Triple<>(
                "Initialize SED " + deviceName,
                "Failed to initialized SED encryption on drive: " + deviceName,
                new String[] {
                    "sedutil-cli",
                    "--initialsetup",
                    password,
                    deviceName
                }));

            cmds.add(new Triple<>(
                "SED enablelockingrange " + deviceName,
                "Failed to enable SED lockingrange on drive: " + deviceName,
                new String[] {
                    "sedutil-cli",
                    "--enablelockingrange",
                    "0",
                    password,
                    deviceName
                }));

            cmds.add(new Triple<>(
                "SED setLockingRange " + deviceName,
                "Failed to set SED lockingrange on drive: " + deviceName,
                new String[] {
                    "sedutil-cli",
                    "--setLockingRange",
                    "0",
                    "LK",
                    password,
                    deviceName
                }));

            cmds.add(new Triple<>(
                "SED setMBRDone " + deviceName,
                "Failed to set SED MBRDone off on drive: " + deviceName,
                new String[]{
                    "sedutil-cli",
                    "--setMBRDone",
                    "off",
                    password,
                    deviceName
                }));

            cmds.add(new Triple<>(
                "SED setMBREnable " + deviceName,
                "Failed to set SED setMBREnable off on drive: " + deviceName,
                new String[]{
                    "sedutil-cli",
                    "--setMBREnable",
                    "off",
                    password,
                    deviceName
                }));

            for (Triple<String, String, String[]> cmd : cmds)
            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                errorReporter.logInfo(cmd.objA);
                var outputData = Commands.genericExecutor(
                    extCmd,
                    cmd.objC,
                    cmd.objB,
                    cmd.objB
                );
                if (outputData.exitCode != 0)
                {
                    throw new StorageException("Failed command: " + cmd.objA);
                }
            }

            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(
                ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT | ApiConsts.MASK_PHYSICAL_DEVICE,
                String.format("initialized SED encryption on drive: %s", deviceName)));
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
            apiCallRc.addEntry(ApiCallRcImpl.copyFromLinstorExc(ApiConsts.FAIL_UNKNOWN_ERROR, storExc));
        }

        return !apiCallRc.hasErrors();
    }

    /**
     * Returns the resolved final path of the device.
     * @param errorReporter
     * @param devicePath
     * @return realpath result for devicePath, if an IOException happens we return the original path
     */
    public static String realpath(
        final ErrorReporter errorReporter,
        final String devicePath)
    {
        String realPath;
        try
        {
            realPath = Paths.get(devicePath).toRealPath().toString();
        }
        catch (IOException ioExc)
        {
            errorReporter.logError("Unable to resolve realpath for " + devicePath);
            errorReporter.reportError(ioExc);
            realPath = devicePath;
        }
        return realPath;
    }

    public static void unlockSED(
        final ExtCmdFactory extCmdFactory,
        final ErrorReporter errorReporter,
        final String deviceName,
        final String password)
    {
        try
        {
            errorReporter.logInfo("Unlock SED %s", deviceName);
            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                // set locking range to rw
                final String failMsg = "Failed to setlockingrange to RW on SED: " + deviceName;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--setlockingrange",
                        "0",
                        "rw",
                        password,
                        deviceName
                    },
                    failMsg,
                    failMsg
                );
            }

            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                final String failMsgMbrDone = "Failed to setmbrdone on SED: " + deviceName;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--setmbrdone",
                        "on",
                        password,
                        deviceName
                    },
                    failMsgMbrDone,
                    failMsgMbrDone
                );
            }

            errorReporter.logInfo("Unlocked SED: %s", deviceName);
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
        }
    }

    public static void revertSEDLocking(
        final ExtCmdFactory extCmdFactory,
        final ErrorReporter errorReporter,
        final String deviceName,
        final String password)
    {
        try
        {
            errorReporter.logInfo("Revert with no erase SED %s", deviceName);
            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                // first revertnoerase
                final String failMsg = "Failed to revertnoerase on SED: " + deviceName + " password is " + password;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--revertnoerase",
                        password,
                        deviceName
                    },
                    failMsg,
                    failMsg
                );
            }


            {
                // now the second sequence with reverttper
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                final String failMsgMbrDone = "Failed to reverttper on SED: " + deviceName + " password is " + password;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--reverttper",
                        password,
                        deviceName
                    },
                    failMsgMbrDone,
                    failMsgMbrDone
                );
            }

            errorReporter.logInfo("Reverted SED: %s", deviceName);
        }
        catch (StorageException storExc)
        {
            errorReporter.reportError(storExc);
        }
    }

    public static void changeSEDPassword(
        final ExtCmdFactory extCmdFactory,
        final ErrorReporter errorReporter,
        final String deviceName,
        final String password,
        final String newPassword)
    {
        try
        {
            errorReporter.logInfo("Changing SED passwords for %s", deviceName);

            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                final String failMsg = "Failed to change SID password on SED: " + deviceName;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--setSIDPassword",
                        password,
                        newPassword,
                        deviceName
                    },
                    failMsg,
                    failMsg
                );
            }

            {
                ExtCmd extCmd = extCmdFactory.create();
                extCmd.logExecution(LOG_SED_COMMANDS);
                final String failMsgAdminPass = "Failed to change Admin1 password on SED: " + deviceName;
                Commands.genericExecutor(
                    extCmd,
                    new String[]{
                        "sedutil-cli",
                        "--setAdmin1Pwd",
                        password,
                        newPassword,
                        deviceName
                    },
                    failMsgAdminPass,
                    failMsgAdminPass
                );
            }

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
            if (fullKey.startsWith(ApiConsts.NAMESPC_SED + ReadOnlyProps.PATH_SEPARATOR))
            {
                final String drive = fullKey.substring(ApiConsts.NAMESPC_SED.length());
                final String sedEncPassword = sedProps.get(fullKey);
                driveMap.put(drive, sedEncPassword);
            }
        }
        return driveMap;
    }
}
