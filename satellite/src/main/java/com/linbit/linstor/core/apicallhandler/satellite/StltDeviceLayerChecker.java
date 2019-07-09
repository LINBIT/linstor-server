package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.drbd.DrbdVersion;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.satellite.authentication.AuthenticationResult;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.StartupVerification;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Every {@link DeviceLayerKind} has a (possibly empty) list of {@link StartupVerification}.
 * Only if all verifications of a list are passed, the satellite will report to being able to have
 * that layer.
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 *
 */
@Singleton
public class StltDeviceLayerChecker
{
    private final DrbdVersion drbdVersionCheck;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public StltDeviceLayerChecker(
        DrbdVersion drbdVersionCheckRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        drbdVersionCheck = drbdVersionCheckRef;
        extCmdFactory = extCmdFactoryRef;
    }

    public AuthenticationResult getSupportedLayerAndProvider(
        String nodeName
    )
    {
        List<DeviceLayerKind> supportedDeviceLayers = new ArrayList<>();
        List<DeviceProviderKind> supportedDeviceProviders = new ArrayList<>();

        List<StartupVerification> satisfiedStartupVerificationChecks = new ArrayList<>();
        for (StartupVerification devLayerVerification : StartupVerification.values())
        {
            boolean isCheckSatisfied;
            switch (devLayerVerification)
            {
                case CRYPT_SETUP:
                    isCheckSatisfied = isCryptSetupInstalled();
                    break;
                case DRBD9:
                    drbdVersionCheck.checkVersion();
                    isCheckSatisfied = drbdVersionCheck.hasDrbd9();
                    break;
                case DRBD_PROXY:
                    isCheckSatisfied = isDrbdProxyInstalled();
                    break;
                case LVM:
                    isCheckSatisfied = isLvmInstalled();
                    break;
                case UNAME:
                    isCheckSatisfied = unameEqualsTo(nodeName);
                    break;
                case ZFS:
                    isCheckSatisfied = isZfsInstalled();
                    break;
                case NVME:
                    isCheckSatisfied = hasNvme();
                    break;
                default:
                    throw new ImplementationError("Unhandled startupVerification: \'" + devLayerVerification + "\'");
            }
            if (isCheckSatisfied)
            {
                satisfiedStartupVerificationChecks.add(devLayerVerification);
            }
        }

        for (DeviceLayerKind devLayerKind : DeviceLayerKind.values())
        {
            if (allChecksSatisfied(satisfiedStartupVerificationChecks, devLayerKind.getStartupVerifications()))
            {
                supportedDeviceLayers.add(devLayerKind);
            }
        }
        // the loop above will always add STORAGE kind as it has no checks.

        for (DeviceProviderKind devProviderKind : DeviceProviderKind.values())
        {
            if (!devProviderKind.equals(DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER) &&
                allChecksSatisfied(satisfiedStartupVerificationChecks, devProviderKind.getStartupVerifications())
            )
            {
                supportedDeviceProviders.add(devProviderKind);
            }
        }
        if (supportedDeviceProviders.isEmpty())
        {
            // if no deviceProvider is supported, the STORAGE layer is not supported
            supportedDeviceLayers.remove(DeviceLayerKind.STORAGE);
        }

        return new AuthenticationResult(
            supportedDeviceLayers,
            supportedDeviceProviders,
            ApiCallRcImpl.singletonApiCallRc(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.CREATED | ApiConsts.MASK_NODE,
                    "successfully authenticated"
                )
            )
        );
    }

    private boolean allChecksSatisfied(
        List<StartupVerification> satisfiedStartupVerificationChecksRef,
        StartupVerification[] startupVerificationsRef
    )
    {
        boolean allChecksSatisfied = true;
        for (StartupVerification sv : startupVerificationsRef)
        {
            if (!satisfiedStartupVerificationChecksRef.contains(sv))
            {
                allChecksSatisfied = false;
                break;
            }
        }
        return allChecksSatisfied;
    }

    private boolean hasNvme()
    {
        return commandExists("modprobe", "nvmet_rdma") && // nvme target over RMDA (implicit nvmet)
            commandExists("modprobe", "nvme_rdma") && // nvme initiator over RDMA (implicit initator)
            commandExists("nvme", "version");
    }

    private boolean isDrbdProxyInstalled()
    {
        return commandExists("drbd-proxy", "-v");
    }

    private boolean isCryptSetupInstalled()
    {
        return commandExists("cryptsetup", "--version");
    }

    private boolean isLvmInstalled()
    {
        return commandExists("lvm", "version");
    }

    private boolean unameEqualsTo(String nodeNameRef)
    {
        boolean ret = false;
        try
        {
            // parse only the hostname part, if domain empty use as full name
            String hostname = nodeNameRef.contains(".") ?
                nodeNameRef.substring(0, nodeNameRef.indexOf(".")) : nodeNameRef;
            OutputData out = extCmdFactory.create().exec("uname", "-n");
            String uname = new String(out.stdoutData).trim();

            // check if hostname matches or hostname + "." + domain
            ret = out.exitCode == 0 &&
                (uname.equalsIgnoreCase(hostname) || uname.equalsIgnoreCase(nodeNameRef));
        }
        catch (ChildProcessTimeoutException | IOException ignored)
        {
        }
        return ret;
    }

    private boolean isZfsInstalled()
    {
        return commandExists("zfs", "--help") && commandExists("zpool", "--help");
    }

    private boolean commandExists(String...command)
    {
        boolean ret = false;
        try
        {
            OutputData out = extCmdFactory.create().exec(command);
            ret = out.exitCode == 0;
        }
        catch (ChildProcessTimeoutException | IOException ignored)
        {
        }
        return ret;
    }
}
