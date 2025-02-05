package com.linbit.linstor.core.apicallhandler;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.Platform;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.layer.drbd.drbdstate.DrbdEventService;
import com.linbit.linstor.layer.storage.ebs.EbsInitiatorProvider;
import com.linbit.linstor.layer.storage.utils.SysClassUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.layer.storage.spdk.utils.SpdkLocalCommands.SPDK_RPC_SCRIPT;

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
import java.util.function.Supplier;
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
        .compile("(?:[Dd]rbd-proxy(?:-rs)? )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern CRYPTSETUP_VERSION_PATTERN = Pattern
        .compile("(?:cryptsetup )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern LVM_VERSION_PATTERN = Pattern
        .compile("(?:\\s*LVM version:\\s*)(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern LVM_THIN_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern THIN_SEND_RECV_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)");
    private static final Pattern ZFS_KMOD_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern ZFS_UTILS_VERSION_PATTERN = Pattern
        .compile("^zfs-(\\d+)\\.(\\d+)\\.(\\d+)(-?)(.*)$", Pattern.MULTILINE);
    private static final Pattern NVME_VERSION_PATTERN = Pattern
        .compile("(?:nvme version\\s*)(\\d+)\\.(\\d+)");
    private static final Pattern SPDK_VERSION_PATTERN = Pattern
        .compile("(?:\\s*version\\s*)?(\\d+)\\.(\\d+)");
    private static final Pattern LOSETUP_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?");
    private static final Pattern ZSTD_VERSION_PATTERN = Pattern.compile("v(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern SOCAT_VERSION_PATTERN = Pattern.compile("version (\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern COREUTILS_VERSION_PATTERN = Pattern.compile(
        "timeout \\(GNU coreutils\\) (\\d+)\\.(\\d+)"
    );
    private static final Pattern UDEVADM_VERSION_PATTERN = Pattern.compile("(\\d+)");
    private static final Pattern LSSCSI_VERSION_PATTERN = Pattern.compile("(?:version: )?(\\d+)\\.(\\d+)");
    private static final String PLATFORM_LINUX = "Linux";
    private static final String PLATFORM_WINDOWS = "Windows";

    private final ErrorReporter errorReporter;
    private final DrbdVersion drbdVersionCheck;
    private final ExtCmdFactory extCmdFactory;
    private final StltConfig stltCfg;

    private @Nullable Map<ExtTools, ExtToolsInfo> cache = null;

    private final DrbdEventService drbdEventService;
    private final Predicate<ExtToolsInfoChecker> platformPredicate;

    @Inject
    public StltExtToolsChecker(
        ErrorReporter errorReporterRef,
        DrbdVersion drbdVersionCheckRef,
        ExtCmdFactory extCmdFactoryRef,
        StltConfig stltCfgRef,
        DrbdEventService drbdEventServiceRef
    )
    {
        errorReporter = errorReporterRef;
        drbdVersionCheck = drbdVersionCheckRef;
        extCmdFactory = extCmdFactoryRef;
        stltCfg = stltCfgRef;
        drbdEventService = drbdEventServiceRef;

        platformPredicate = getPlatformPredicate();
    }

    private static class ExtToolsInfoChecker
    {
        /*
         * Deliberately using a supplier here instead of the resulting ExtToolsInfo instance because of two reasons:
         * 1) to be able to perform the isLinux/isWindows test before running the supplier/check
         * 2) so that the logged time of the "supported" / "NOT supported" messages correlate with the check itself. If
         * a check takes unusually long, this way we should have at least some hints which ExtTool-check is causing the
         * issue
         */
        private final Supplier<ExtToolsInfo> supplier;
        private final boolean hasLinuxTarget;
        private final boolean hasWindowsTarget;

        ExtToolsInfoChecker(Supplier<ExtToolsInfo> supplierRef, boolean hasLinuxTargetRef, boolean hasWindowsTargetRef)
        {
            supplier = supplierRef;
            hasLinuxTarget = hasLinuxTargetRef;
            hasWindowsTarget = hasWindowsTargetRef;
        }

        boolean hasLinuxTarget()
        {
            return hasLinuxTarget;
        }

        boolean hasWindowsTarget()
        {
            return hasWindowsTarget;
        }

        static ExtToolsInfoChecker checkBoth(Supplier<ExtToolsInfo> supplierRef)
        {
            return new ExtToolsInfoChecker(supplierRef, true, true);
        }

        static ExtToolsInfoChecker checkLinux(Supplier<ExtToolsInfo> supplierRef)
        {
            return new ExtToolsInfoChecker(supplierRef, true, false);
        }

        static ExtToolsInfoChecker checkWindows(Supplier<ExtToolsInfo> supplierRef)
        {
            return new ExtToolsInfoChecker(supplierRef, false, true);
        }
    }

    private ExtToolsInfoChecker[] getCheckerArray()
    {
        final List<String> loadedModules;
        if (Platform.isLinux())
        {
            loadedModules = getLoadedModules();
        }
        else
        {
            loadedModules = Collections.emptyList();
        }

        return new ExtToolsInfoChecker[] {
            ExtToolsInfoChecker.checkBoth(this::getDrbd9Info),
            ExtToolsInfoChecker.checkBoth(this::getDrbdUtilsInfo),
            ExtToolsInfoChecker.checkWindows(this::getStorageSpacesInfo),
            ExtToolsInfoChecker.checkLinux(this::getDrbdProxyInfo),
            ExtToolsInfoChecker.checkLinux(this::getCryptSetupInfo),
            ExtToolsInfoChecker.checkLinux(this::getLvmInfo),
            ExtToolsInfoChecker.checkLinux(this::getLvmThinInfo),
            ExtToolsInfoChecker.checkLinux(this::getThinSendRecvInfo),
            ExtToolsInfoChecker.checkLinux(this::getZfsKmodInfo),
            ExtToolsInfoChecker.checkLinux(this::getZfsUtilsInfo),
            ExtToolsInfoChecker.checkLinux(() -> getNvmeInfo(loadedModules)),
            ExtToolsInfoChecker.checkLinux(this::getSpdkInfo),
            ExtToolsInfoChecker.checkLinux(this::getEbsTargetInfo),
            ExtToolsInfoChecker.checkLinux(this::getEbsInitInfo),
            ExtToolsInfoChecker.checkLinux(() -> getWritecacheInfo(loadedModules)),
            ExtToolsInfoChecker.checkLinux(() -> getCacheInfo(loadedModules)),
            ExtToolsInfoChecker.checkLinux(() -> getBCacheInfo(loadedModules)),
            ExtToolsInfoChecker.checkLinux(this::getLosetupInfo),
            ExtToolsInfoChecker.checkLinux(this::getZstdInfo),
            ExtToolsInfoChecker.checkLinux(this::getSocatInfo),
            ExtToolsInfoChecker.checkLinux(this::getCoreUtilsInfo),
            ExtToolsInfoChecker.checkLinux(this::getUdevadmInfo),
            ExtToolsInfoChecker.checkLinux(this::getLsscsiInfo),
            ExtToolsInfoChecker.checkLinux(this::getSasPhyInfo),
            ExtToolsInfoChecker.checkLinux(this::getSasDeviceInfo),
        };
    }

    public Map<ExtTools, ExtToolsInfo> getExternalTools(boolean recache)
    {
        if (recache || cache == null)
        {
            boolean wasDrbd9AvailableBeforeRecheck = drbdVersionCheck.hasDrbd9();

            // needed by getDrbd9Info() and getDrbdUtilsInfo(). however, calling checkVersions() once is enough
            drbdVersionCheck.checkVersions();

            if (!wasDrbd9AvailableBeforeRecheck && drbdVersionCheck.hasDrbd9() && !drbdEventService.isStarted())
            {
                drbdEventService.start();
            }

            Map<ExtTools, ExtToolsInfo> extTools = new HashMap<>();
            for (ExtToolsInfoChecker checker : getCheckerArray())
            {
                if (platformPredicate.test(checker))
                {
                    ExtToolsInfo info = checker.supplier.get();
                    extTools.put(info.getTool(), info);
                    logSupported(info);
                }
            }
            for (ExtTools tool : ExtTools.values())
            {
                extTools.putIfAbsent(tool, doesNotExist(tool));
            }

            cache = Collections.unmodifiableMap(extTools);
        }
        return cache;
    }

    private Predicate<ExtToolsInfoChecker> getPlatformPredicate()
        throws ImplementationError
    {
        Predicate<ExtToolsInfoChecker> ret;
        if (Platform.isLinux())
        {
            ret = ExtToolsInfoChecker::hasLinuxTarget;
        }
        else if (Platform.isWindows())
        {
            ret = ExtToolsInfoChecker::hasWindowsTarget;
        }
        else
        {
            throw new ImplementationError(
                "Platform is neither Linux nor Windows, please add support for it to LINSTOR"
            );
        }
        return ret;
    }

    private void logSupported(ExtToolsInfo info)
    {
        if (info.isSupported())
        {
            errorReporter.logInfo(
                "Checking support for %s: supported (%s)",
                info.getTool().name(),
                info.getVersion().toString()
            );
        }
        else
        {
            errorReporter.logInfo("Checking support for %s: NOT supported:", info.getTool().name());
            for (String reason : info.getNotSupportedReasons())
            {
                errorReporter.logDebug("   %s", reason);
            }
        }
    }

    public boolean areSupported(boolean recache, ExtTools... tools)
    {
        boolean supported = true;
        Map<ExtTools, ExtToolsInfo> extTools = getExternalTools(recache);
        for (ExtTools tool : tools)
        {
            ExtToolsInfo extToolsInfo = extTools.get(tool);
            if (extToolsInfo == null || !extToolsInfo.isSupported())
            {
                supported = false;
                break;
            }
        }
        return supported;
    }

    private ExtToolsInfo doesNotExist(ExtTools tool)
    {
        List<String> reasons = new ArrayList<>();
        reasons.add(
            String.format(
                "This tool does not exist on the %s platform.",
                Platform.isLinux() ? PLATFORM_LINUX : PLATFORM_WINDOWS
            )
        );

        return new ExtToolsInfo(tool, false, null, null, null, reasons);
    }

    private ExtToolsInfo getDrbd9Info()
    {
        ExtToolsInfo drbdInfo;
        if (drbdVersionCheck.hasDrbd9())
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9_KERNEL,
                true,
                drbdVersionCheck.getKModVsn(),
                Collections.emptyList()
            );
        }
        else
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9_KERNEL,
                false,
                null,
                null,
                null,
                drbdVersionCheck.getKernelNotSupportedReasons()
            );
        }
        return drbdInfo;
    }

    private ExtToolsInfo getDrbdUtilsInfo()
    {
        ExtToolsInfo drbdInfo;
        if (drbdVersionCheck.hasUtils())
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9_UTILS,
                true,
                drbdVersionCheck.getUtilsVsn(),
                Collections.emptyList()
            );
        }
        else
        {
            drbdInfo = new ExtToolsInfo(
                ExtTools.DRBD9_UTILS,
                false,
                null,
                null,
                null,
                drbdVersionCheck.getUtilsNotSupportedReasons()
            );
        }
        String windrbdVersion = drbdVersionCheck.getWindrbdVsn();
        if (windrbdVersion != null)
        {
            errorReporter.logTrace("Found WinDRBD version " + windrbdVersion + "\n");
        }
        return drbdInfo;
    }

    private ExtToolsInfo getStorageSpacesInfo()
    {
        ExtToolsInfo extToolsInfo;
        Either<Pair<String, String>, List<String>> stdoutOrErrorReason = getStdoutOrErrorReason(
            ec -> ec == 0,
            "linstor-wmi-helper",
            "storage-pool",
            "list"
        );

        extToolsInfo = stdoutOrErrorReason.map(
            pair ->
            {
                return new ExtToolsInfo(
                    ExtTools.STORAGE_SPACES,
                    true,
                    null,
                    null,
                    null,
                    Collections.emptyList());
            },
            notSupportedReasonList -> new ExtToolsInfo(
                ExtTools.STORAGE_SPACES,
                false,
                null,
                null,
                null,
                notSupportedReasonList
            )
        );

        return extToolsInfo;
    }

    private ExtToolsInfo getDrbdProxyInfo()
    {
        return infoBy3MatchGroupPattern(DRBD_PROXY_VERSION_PATTERN, ExtTools.DRBD_PROXY, "drbd-proxy", "--version");
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

    private ExtToolsInfo getZfsKmodInfo()
    {
        return infoBy3MatchGroupPattern(ZFS_KMOD_VERSION_PATTERN, ExtTools.ZFS_KMOD, "cat", "/sys/module/zfs/version");
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private ExtToolsInfo getZfsUtilsInfo()
    {
        // Although older zfs versions have neither '--version' nor 'version' subcommand, we still try a 'zfs --version'
        // first. If that succeeds, we are happy to know the precise version.
        // If that fails (should return an exit-code of 2 (subcommand not found) or 1 (some other error occurred)), we
        // additionally run a 'zfs -?'. If that has an exit code of 0, zfs utils is available but in a version
        // pre 0.8.0 (before --version was introduced)
        ExtToolsInfo ret = getStdoutOrErrorReason(
            ec -> ec == 0 || ec == 1 || ec == 2,
            "zfs", "--version"
        ).map(
            pair ->
            {
                Version version;

                Matcher matcher = ZFS_UTILS_VERSION_PATTERN.matcher(pair.objA);
                boolean found = matcher.find();
                if (!found)
                {
                    matcher = ZFS_UTILS_VERSION_PATTERN.matcher(pair.objB); // retry in stderr output
                    found = matcher.find();
                }
                if (found)
                {
                    version = new ExtToolsInfo.Version(
                        Integer.parseInt(matcher.group(1)),
                        Integer.parseInt(matcher.group(2)),
                        Integer.parseInt(matcher.group(3)),
                        matcher.group(4),
                        matcher.group(5)
                    );
                }
                else
                {
                    // zfs is supported but "zfs --vesion" does not exist. unknown version (most likely pre 0.8.0)
                    version = new ExtToolsInfo.Version();
                }
                return new ExtToolsInfo(
                    ExtTools.ZFS_UTILS,
                    true,
                    version,
                    Collections.emptyList()
                );
            },
            notSupportedReasonList -> new ExtToolsInfo(
                ExtTools.ZFS_UTILS,
                false,
                null,
                null,
                null,
                notSupportedReasonList
            )
        );

        return ret;
    }

    private ExtToolsInfo getNvmeInfo(List<String> loadedModulesRef)
    {
        ExtToolsInfo ret;

        if (stltCfg.isRemoteSpdk())
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
        return infoBy3MatchGroupPattern(
            SPDK_VERSION_PATTERN,
            ExtTools.SPDK,
            false,
            SPDK_RPC_SCRIPT,
            "spdk_get_version" // "get_spdk_version" is deprecated
        );
    }

    private ExtToolsInfo getEbsTargetInfo()
    {
        boolean isSupported = stltCfg.isEbs();
        return new ExtToolsInfo(
            ExtTools.EBS_TARGET,
            isSupported,
            new Version(),
            isSupported ? Collections.emptyList() : Arrays.asList("Satellite is not a (special) EBS satellite")
        );
    }

    private ExtToolsInfo getEbsInitInfo()
    {
        final @Nullable String ec2InstanceId = EbsInitiatorProvider.getEc2InstanceId(errorReporter, extCmdFactory);
        boolean isSupported = ec2InstanceId != null;
        return new ExtToolsInfo(
            ExtTools.EBS_INIT,
            isSupported,
            new Version(),
            isSupported ?
                Collections.emptyList() :
                Arrays.asList("Failed to retrieve instance-id from " + EbsInitiatorProvider.EC2_INSTANCE_ID_PATH)
        );
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

    private ExtToolsInfo getBCacheInfo(List<String> loadedModulesRef)
    {
        ExtToolsInfo extToolsInfo;

        List<String> failReasons = new ArrayList<>();
        checkModuleLoaded(loadedModulesRef, "bcache", failReasons);
        if (!failReasons.isEmpty())
        {
            extToolsInfo = new ExtToolsInfo(ExtTools.BCACHE_TOOLS, false, null, null, null, failReasons);
        }
        else
        {
            Either<Pair<String, String>, List<String>> stdoutOrErrorReason = getStdoutOrErrorReason(
                ec -> ec == 0 || ec == 1,
                "make-bcache",
                "-h"
            );

            extToolsInfo = stdoutOrErrorReason.map(
                pair -> new ExtToolsInfo(
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

    private ExtToolsInfo getCoreUtilsInfo()
    {
        return infoBy3MatchGroupPattern(
            COREUTILS_VERSION_PATTERN,
            ExtTools.COREUTILS_LINUX,
            false,
            "timeout",
            "--version"
        );
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

    @SuppressWarnings("checkstyle:MagicNumber")
    private ExtToolsInfo infoBy3MatchGroupPattern(
        Pattern pattern,
        ExtTools tool,
        boolean hasMinorVersion,
        boolean hasPatchVersion,
        boolean parseStdOut,
        String... cmd
    )
    {
        return getStdoutOrErrorReason(cmd).map(
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
                            "Failed to parse version of installed '" + StringUtils.joinShellQuote(cmd) + "'",
                            "Standard out: '" + pair.objA.trim() + "'",
                            "Standard err: '" + pair.objB.trim() + "'"
                        )
                    );
                }
                return ret;
            },
            notSupportedReasonList -> new ExtToolsInfo(tool, false, null, null, null, notSupportedReasonList)
        );
    }

    private void checkModuleLoaded(List<String> loadedModulesRef, String moduleName, List<String> failReasons)
    {
        String listedModuleName = moduleName.replaceAll("-", "_");
        if (!loadedModulesRef.contains(listedModuleName))
        {
            check(failReasons, "modprobe", moduleName);
        }
        else
        {
            errorReporter.logTrace("Module '%s' already loaded (found in /proc/modules)", listedModuleName);
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
                        "'" + StringUtils.joinShellQuote(cmds) + "' returned with exit code " + out.exitCode
                    )
                );
            }
        }
        catch (IOException ioExc)
        {
            ret = Either.right(
                Arrays.asList(
                    "IO exception occured when running '" + StringUtils.joinShellQuote(cmds) + "': " +
                        ioExc.getMessage()
                )
            );
            // errorReporter.reportError(ioExc);
        }
        catch (ChildProcessTimeoutException timeoutExc)
        {
            ret = Either.right(
                Arrays.asList(
                    "'" + StringUtils.joinShellQuote(cmds) + "' timed out."
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
