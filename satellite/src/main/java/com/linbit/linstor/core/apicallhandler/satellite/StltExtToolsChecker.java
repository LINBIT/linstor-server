package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.storage.utils.SpdkCommands.SPDK_RPC_SCRIPT;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    private static final Pattern DRBD_PROXY_VERSION_PATTERN = Pattern
        .compile("(?:Drbd-proxy )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern CRYPTSETUP_VERSION_PATTERN = Pattern
        .compile("(?:cryptsetup )?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern LVM_VERSION_PATTERN = Pattern
        .compile("(?:\\s*LVM vesion:\\s*)?(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern ZFS_VERSION_PATTERN = Pattern
        .compile("(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern NVME_VERSION_PATTERN = Pattern
        .compile("(?:nvme version\\s*)(\\d+)\\.(\\d+)");
    private static final Pattern SPDK_VERSION_PATTERN = Pattern
            .compile("(?:\\s*version\\s*)?(\\d+)\\.(\\d+)");

    private final ErrorReporter errorReporter;
    private final DrbdVersion drbdVersionCheck;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public StltExtToolsChecker(
        ErrorReporter errorReporterRef,
        DrbdVersion drbdVersionCheckRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        drbdVersionCheck = drbdVersionCheckRef;
        extCmdFactory = extCmdFactoryRef;
    }

    public List<ExtToolsInfo> getExternalTools()
    {
        return Arrays.asList(
            getDrbd9Info(),
            getDrbdProxyInfo(),
            getCryptSetupInfo(),
            getLvmInfo(),
            getZfsInfo(),
            getNvmeInfo(),
            getSpdkInfo(),
            getWritecacheInfo()
        );
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
                Integer.valueOf(drbdVersionCheck.getMajorVsn()),
                Integer.valueOf(drbdVersionCheck.getMinorVsn()),
                Integer.valueOf(drbdVersionCheck.getPatchLvl()),
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

    private ExtToolsInfo getZfsInfo()
    {
        return infoBy3MatchGroupPattern(ZFS_VERSION_PATTERN, ExtTools.ZFS, "cat", "/sys/module/zfs/version");
    }

    private ExtToolsInfo getNvmeInfo()
    {
        ExtToolsInfo ret;

        List<String> modprobeFailures = new ArrayList<>();
        check(modprobeFailures, "modprobe", "nvmet_rdma");
        check(modprobeFailures, "modprobe", "nvme_rdma");
        if (!modprobeFailures.isEmpty())
        {
            ret = new ExtToolsInfo(ExtTools.NVME, false, null, null, null, modprobeFailures);
        }
        else
        {
            ret = infoBy3MatchGroupPattern(NVME_VERSION_PATTERN, ExtTools.NVME, false, "nvme", "version");
        }
        return ret;
    }

    private ExtToolsInfo getSpdkInfo()
    {
        return infoBy3MatchGroupPattern(SPDK_VERSION_PATTERN, ExtTools.SPDK, false, SPDK_RPC_SCRIPT, "get_spdk_version");
    }

    private ExtToolsInfo getWritecacheInfo()
    {
        Either<Pair<String, String>, List<String>> stdoutOrErrorReason = getStdoutOrErrorReason("modinfo", "dm-writecache");
        return stdoutOrErrorReason.map(
            pair -> new ExtToolsInfo(ExtTools.WRITECACHE, true, null, null, null, Collections.emptyList()),
            errorReson -> new ExtToolsInfo(ExtTools.WRITECACHE, false, null, null, null, errorReson)
        );
    }

    private ExtToolsInfo infoBy3MatchGroupPattern(Pattern pattern, ExtTools tool, String... cmd)
    {
        return infoBy3MatchGroupPattern(pattern, tool, true, cmd);
    }

    private ExtToolsInfo infoBy3MatchGroupPattern(
        Pattern pattern,
        ExtTools tool,
        boolean hasPatchVersion,
        String... cmd
    )
    {
        return getStdoutOrErrorReason(cmd).map(
            pair ->
            {
                ExtToolsInfo ret;
                Matcher match = pattern.matcher(pair.objA);
                if (match.find())
                {
                    ret = new ExtToolsInfo(
                        tool,
                        true,
                        Integer.parseInt(match.group(1)),
                        Integer.parseInt(match.group(2)),
                        hasPatchVersion ? Integer.parseInt(match.group(3)) : null,
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
            OutputData out = extCmdFactory.create().exec(cmds);
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
}
