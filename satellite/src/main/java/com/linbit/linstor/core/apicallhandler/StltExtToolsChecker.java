package com.linbit.linstor.core.apicallhandler;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.layer.storage.utils.SysClassUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.layer.storage.spdk.utils.SpdkCommands.SPDK_RPC_SCRIPT;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Every {@link DeviceLayerKind} has a (possibly empty) list of {@link ExtTools}.
 * Only if all verifications of a list are passed, the satellite will report to being able to have
 * that layer.
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 */
@Singleton
public class StltExtToolsChecker
{
    private static final Pattern PROC_MODULES_NAME_PATTERN = Pattern.compile("^([^ ]+)", Pattern.MULTILINE);

    private static final Pattern DRBD_PROXY_VERSION_PATTERN = Pattern
        .compile("(?:Drbd-proxy )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern CRYPTSETUP_VERSION_PATTERN = Pattern
        .compile("(?:cryptsetup )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern LVM_VERSION_PATTERN = Pattern
        .compile("(?:\\s*LVM version:\\s*)(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern LVM_THIN_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern THIN_SEND_RECV_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");
    private static final Pattern ZFS_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern NVME_VERSION_PATTERN = Pattern
        .compile("(?:nvme version\\s*)(\\d+)\\.(\\d+)");
    private static final Pattern SPDK_VERSION_PATTERN = Pattern
        .compile("(?:\\s*version\\s*)?(\\d+)\\.(\\d+)");
    private static final Pattern LOSETUP_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?");
    private static final Pattern ZSTD_VERSION_PATTERN = Pattern.compile("v(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern SOCAT_VERSION_PATTERN = Pattern.compile("version (\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern UTIL_LINUX_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");
    private static final Pattern UDEVADM_VERSION_PATTERN = Pattern.compile("(\\d+)");
    private static final Pattern LSSCSI_VERSION_PATTERN = Pattern.compile("(?:version: )?(\\d+)\\.(\\d+)");

    private final ErrorReporter errorReporter;
    private final DrbdVersion drbdVersionCheck;
    private final ExtCmdFactory extCmdFactory;
    private final StltConfig stltCfg;

    private Map<ExtTools, ExtToolsInfo> cache = null;

    @Inject
    public StltExtToolsChecker(
        ErrorReporter errorReporterRef,
        DrbdVersion drbdVersionCheckRef,
        ExtCmdFactory extCmdFactoryRef,
        StltConfig stltCfgRef
    )
    {
        errorReporter = errorReporterRef;
        drbdVersionCheck = drbdVersionCheckRef;
        extCmdFactory = extCmdFactoryRef;
        stltCfg = stltCfgRef;
    }

    public Map<ExtTools, ExtToolsInfo> getExternalTools(boolean recache)
    {
        if (recache || cache == null)
        {
            List<String> loadedModules = getLoadedModules();

            ExtToolsInfo[] infoArray =
            {
                getDrbd9Info(),
                getDrbdProxyInfo(),
                getCryptSetupInfo(),
                getLvmInfo(),
                getLvmThinInfo(),
                getThinSendRecvInfo(),
                getZfsInfo(),
                getNvmeInfo(loadedModules),
                getSpdkInfo(),
                getWritecacheInfo(loadedModules),
                getCacheInfo(loadedModules),
                getBCacheInfo(),
                getLosetupInfo(),
                getZstdInfo(),
                getSocatInfo(),
                getUtilLinuxInfo(),
                getUdevadmInfo(),
                getLsscsiInfo(),
                getSasPhyInfo(),
                getSasDeviceInfo()
            };

            Map<ExtTools, ExtToolsInfo> extTools = new HashMap<>();
            for (ExtToolsInfo info : infoArray)
            {
                extTools.put(info.getTool(), info);
            }

            cache = Collections.unmodifiableMap(extTools);
        }
        return cache;
    }

    public boolean areSupported(boolean recache, ExtTools... tools)
    {
        Map<ExtTools, ExtToolsInfo> extTools = getExternalTools(recache);
        for (ExtTools tool : tools)
        {
            ExtToolsInfo extToolsInfo = extTools.get(tool);
            if (extToolsInfo == null || !extToolsInfo.isSupported())
            {
                return false;
            }
        }
        return true;
    }

    private ExtToolsInfo getDrbd9Info()
    {
        ExtToolsInfo drbdInfo;
        drbdVersionCheck.checkVersion();
        if (drbdVersionCheck.hasDrbd9())
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9,
                true,
                (int) drbdVersionCheck.getMajorVsn(),
                (int) drbdVersionCheck.getMinorVsn(),
                (int) drbdVersionCheck.getPatchLvl(),
                Collections.emptyList()
            );
        }
        else
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9,
                false,
                null,
                null,
                null,
                drbdVersionCheck.getNotSupportedReasons()
            );
        }
        return drbdInfo;
    }

    private ExtToolsInfo getDrbdProxyInfo()
    {
        return infoBy3MatchGroupPattern(DRBD_PROXY_VERSION_PATTERN, ExtTools.DRBD_PROXY, "drbd-proxy", "-v");
    }

    private ExtToolsInfo getCryptSetupInfo()
    {
        return infoBy3MatchGroupPattern(
            CRYPTSETUP_VERSION_PATTERN,
            ExtTools.CRYPT_SETUP,
            "cryptsetup", "--version"
        );
    }

    private ExtToolsInfo getLvmInfo()
    {
        return infoBy3MatchGroupPattern(LVM_VERSION_PATTERN, ExtTools.LVM, "lvm", "version");
    }

    private ExtToolsInfo getLvmThinInfo()
    {
        return infoBy3MatchGroupPattern(LVM_THIN_VERSION_PATTERN, ExtTools.LVM_THIN, "thin_check", "-V");
    }

    private ExtToolsInfo getThinSendRecvInfo()
    {
        return infoBy3MatchGroupPattern(
            THIN_SEND_RECV_VERSION_PATTERN,
            ExtTools.THIN_SEND_RECV,
            false,
            "thin_send", "-v"
        );
    }

    private ExtToolsInfo getZfsInfo()
    {
        return infoBy3MatchGroupPattern(ZFS_VERSION_PATTERN, ExtTools.ZFS, "cat", "/sys/module/zfs/version");
    }

    private ExtToolsInfo getNvmeInfo(List<String> loadedModulesRef)
    {
        ExtToolsInfo ret;

        if (stltCfg.isOpenflex())
        {
            ret = new ExtToolsInfo(ExtTools.NVME, true, null, null, null, null);
        }
        else
        {
            List<String> modprobeFailures = new ArrayList<>();
            checkModuleLoaded(loadedModulesRef, "nvmet_rdma", modprobeFailures);
            checkModuleLoaded(loadedModulesRef, "nvme_rdma", modprobeFailures);
            if (!modprobeFailures.isEmpty())
            {
                ret = new ExtToolsInfo(ExtTools.NVME, false, null, null, null, modprobeFailures);
            }
            else
            {
                ret = infoBy3MatchGroupPattern(NVME_VERSION_PATTERN, ExtTools.NVME, false, "nvme", "version");
            }
        }
        return ret;
    }

    private ExtToolsInfo getSpdkInfo()
    {
        return infoBy3MatchGroupPattern(SPDK_VERSION_PATTERN, ExtTools.SPDK, false, SPDK_RPC_SCRIPT, "get_spdk_version");
    }

    private ExtToolsInfo getWritecacheInfo(List<String> loadedModulesRef)
    {
        List<String> errorList = new ArrayList<>();
        checkModuleLoaded(loadedModulesRef, "dm-writecache", errorList);
        return new ExtToolsInfo(ExtTools.DM_WRITECACHE, errorList.isEmpty(), null, null, null, errorList);
    }

    private ExtToolsInfo getCacheInfo(List<String> loadedModulesRef)
    {
        List<String> errorList = new ArrayList<>();
        checkModuleLoaded(loadedModulesRef, "dm-cache", errorList);
        return new ExtToolsInfo(ExtTools.DM_CACHE, errorList.isEmpty(), null, null, null, errorList);
    }

    private ExtToolsInfo getBCacheInfo()
    {
        Either<Pair<String, String>, List<String>> stdoutOrErrorReason = getStdoutOrErrorReason(
            ec -> ec == 0 || ec == 1,
            "make-bcache",
            "-h"
        );

        ExtToolsInfo extToolsInfo = stdoutOrErrorReason.map(
            pair ->
                new ExtToolsInfo(
                    ExtTools.BCACHE_TOOLS,
                    true,
                    null,
                    null,
                    null,
                    Collections.emptyList()
            ),
            notSupportedReasonList -> new ExtToolsInfo(
                ExtTools.BCACHE_TOOLS,
                false,
                null,
                null,
                null,
                notSupportedReasonList
            )
        );
        if (extToolsInfo.isSupported())
        {
            errorReporter.logTrace(
                "Checking support for %s: supported ",
                ExtTools.BCACHE_TOOLS.name()
            );
        }
        else
        {
            errorReporter.logTrace(
                "Checking support for %s: NOT supported ",
                ExtTools.BCACHE_TOOLS.name()
            );
        }
        return extToolsInfo;
    }

    private ExtToolsInfo getLosetupInfo()
    {
        return infoBy3MatchGroupPattern(LOSETUP_VERSION_PATTERN, ExtTools.LOSETUP, false, "losetup", "--version");
    }

    private ExtToolsInfo getZstdInfo()
    {
        return infoBy3MatchGroupPattern(ZSTD_VERSION_PATTERN, ExtTools.ZSTD, false, "zstd", "-V");
    }

    private ExtToolsInfo getSocatInfo()
    {
        return infoBy3MatchGroupPattern(SOCAT_VERSION_PATTERN, ExtTools.SOCAT, false, "socat", "-V");
    }

    private ExtToolsInfo getUtilLinuxInfo()
    {
        return infoBy3MatchGroupPattern(UTIL_LINUX_VERSION_PATTERN, ExtTools.UTIL_LINUX, false, "setsid", "-V");
    }

    private ExtToolsInfo getUdevadmInfo()
    {
        return infoBy3MatchGroupPattern(
            UDEVADM_VERSION_PATTERN,
            ExtTools.UDEVADM,
            false,
            false,
            true,
            "udevadm",
            "version"
        );
    }

    private ExtToolsInfo getLsscsiInfo()
    {
        return infoBy3MatchGroupPattern(
            LSSCSI_VERSION_PATTERN,
            ExtTools.LSSCSI,
            true,
            false,
            false,
            "lsscsi",
            "--version"
        );
    }

    private ExtToolsInfo getSasPhyInfo()
    {
        List<String> errorList = new ArrayList<>();
        check(errorList, SysClassUtils.CMD_CAT_SAS_PHY);
        return new ExtToolsInfo(ExtTools.SAS_PHY, errorList.isEmpty(), null, null, null, errorList);
    }

    private ExtToolsInfo getSasDeviceInfo()
    {
        List<String> errorList = new ArrayList<>();
        check(errorList, SysClassUtils.CMD_CAT_SAS_DEVICE);
        return new ExtToolsInfo(ExtTools.SAS_DEVICE, errorList.isEmpty(), null, null, null, errorList);
    }

    private ExtToolsInfo infoBy3MatchGroupPattern(Pattern pattern, ExtTools tool, String... cmd)
    {
        return infoBy3MatchGroupPattern(pattern, tool, true, true, true, cmd);
    }

    private ExtToolsInfo infoBy3MatchGroupPattern(
        Pattern pattern,
        ExtTools tool,
        boolean hasPatchVersion,
        String... cmd
    )
    {
        return infoBy3MatchGroupPattern(pattern, tool, true, hasPatchVersion, true, cmd);
    }

    private ExtToolsInfo infoBy3MatchGroupPattern(
        Pattern pattern,
        ExtTools tool,
        boolean hasMinorVersion,
        boolean hasPatchVersion,
        boolean parseStdOut,
        String... cmd
    )
    {
        ExtToolsInfo extToolsInfo = getStdoutOrErrorReason(cmd).map(
            pair ->
            {
                ExtToolsInfo ret;
                Matcher match;
                if (parseStdOut)
                {
                    match = pattern.matcher(pair.objA);
                }
                else
                {
                    match = pattern.matcher(pair.objB);
                }
                if (match.find())
                {
                    ret = new ExtToolsInfo(
                        tool,
                        true,
                        Integer.parseInt(match.group(1)),
                        hasMinorVersion ? Integer.parseInt(match.group(2)) : null,
                        hasMinorVersion && hasPatchVersion ? Integer.parseInt(match.group(3)) : null,
                        Collections.emptyList()
                    );
                }
                else
                {
                    ret = new ExtToolsInfo(
                        tool,
                        false,
                        null,
                        null,
                        null,
                        Arrays.asList(
                            "Failed to parse version of installed '" + StringUtils.join(" ", cmd) + "'",
                            "Standard out: '" + pair.objA.trim() + "'",
                            "Standard err: '" + pair.objB.trim() + "'"
                        )
                    );
                }
                return ret;
            },
            notSupportedReasonList -> new ExtToolsInfo(tool, false, null, null, null, notSupportedReasonList)
        );
        if (extToolsInfo.isSupported())
        {
            errorReporter.logTrace(
                "Checking support for %s: supported (%s)",
                tool.name(),
                extToolsInfo.getVersion().toString()
            );
        }
        else
        {
            errorReporter.logTrace("Checking support for %s: NOT supported:", tool.name());
            for (String reason : extToolsInfo.getNotSupportedReasons())
            {
                errorReporter.logTrace("   %s", reason);
            }
        }
        return extToolsInfo;
    }

    private void checkModuleLoaded(List<String> loadedModulesRef, String moduleName, List<String> failReasons)
    {
        String module_Name = moduleName.replaceAll("-", "_");
        if (!loadedModulesRef.contains(module_Name))
        {
            check(failReasons, "modprobe", moduleName);
        }
        else
        {
            errorReporter.logTrace("Module '%s' already loaded (found in /proc/modules)", module_Name);
        }
    }

    private void check(List<String> resultErrors, String... commandParts)
    {
        Either<Pair<String, String>, List<String>> stdoutOrErrorReason = getStdoutOrErrorReason(commandParts);
        stdoutOrErrorReason.map(
            ignore -> true, // addAll also returns boolean, just to make the <T> of the map method happy
            failList -> resultErrors.addAll(failList)
        );
    }

    private Either<Pair<String, String>, List<String>> getStdoutOrErrorReason(String... cmds)
    {
        return getStdoutOrErrorReason(exitCode -> exitCode == 0, cmds);
    }

    private Either<Pair<String, String>, List<String>> getStdoutOrErrorReason(
        Predicate<Integer> exitCodeTest,
        String... cmds
    )
    {
        Either<Pair<String, String>, List<String>> ret;
        try
        {
            OutputData out = extCmdFactory.create().logExecution(false).exec(cmds);
            if (exitCodeTest.test(out.exitCode))
            {
                ret = Either.left(new Pair<>(new String(out.stdoutData), new String(out.stderrData)));
            }
            else
            {
                ret = Either.right(
                    Arrays.asList(
                        "'" + StringUtils.join(" ", cmds) + "' returned with exit code " + out.exitCode
                    )
                );
            }
        }
        catch (IOException ioExc)
        {
            ret = Either.right(
                Arrays.asList(
                    "IO exception occured when running '" + StringUtils.join(" ", cmds) + "': " +
                        ioExc.getMessage()
                )
            );
            // errorReporter.reportError(ioExc);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            ret = Either.right(
                Arrays.asList(
                    "'" + StringUtils.join(" ", cmds) + "' timed out."
                )
            );
            errorReporter.reportError(timeoutExc);
        }
        return ret;
    }

    private List<String> getLoadedModules()
    {
        List<String> ret = new ArrayList<>();
        try
        {
            errorReporter.logTrace("Caching /proc/modules");
            Matcher matcher = PROC_MODULES_NAME_PATTERN.matcher(
                new String(Files.readAllBytes(Paths.get("/proc/modules")))
            );
            while (matcher.find())
            {
                ret.add(matcher.group(1));
            }
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
        }

        return ret;
    }
}
