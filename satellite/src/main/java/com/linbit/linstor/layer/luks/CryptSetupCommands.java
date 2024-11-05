package com.linbit.linstor.layer.luks;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;
import com.linbit.linstor.storage.utils.Luks;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class CryptSetupCommands implements Luks
{
    private static final String CRYPTSETUP = "cryptsetup";
    private static final String CRYPT_PREFIX = "Linstor-Crypt-";
    private static final Version V2_1_0 = new Version(2, 1, 0);
    private static final Version V2_0_0 = new Version(2, 0, 0);
    private static final String PBDKF_MAX_MEMORY_KIB = "262144";  // 256 MiB

    @SuppressWarnings("unused")
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final Version version;

    @Inject
    public CryptSetupCommands(
        ExtCmdFactory extCmdFactoryRef,
        ErrorReporter errorReporterRef,
        StltExtToolsChecker extToolsCheckerRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;

        ExtToolsInfo cryptSetupInfo = extToolsCheckerRef.getExternalTools(false).get(ExtTools.CRYPT_SETUP);
        if (cryptSetupInfo != null)
        {
            version = cryptSetupInfo.getVersion();
        }
        else
        {
            version = null;
        }

    }

    @Override
    public String createLuksDevice(
        String dev,
        byte[] cryptKey,
        String identifier
    )
        throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            ArrayList<String> command = new ArrayList<>();
            command.add(CRYPTSETUP);
            command.add("-q");
            command.add("luksFormat");
            if (version.greaterOrEqual(V2_0_0))
            {
                command.add("--pbkdf-memory");
                command.add(PBDKF_MAX_MEMORY_KIB); // in kib
            }
            command.add(dev);

            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                null,
                command.toArray(new String[0])
            );
            outputStream.write(cryptKey);
            outputStream.write('\n');
            outputStream.flush();

            OutputData output = extCommand.syncProcess();
            outputStream.close(); // just to be sure and get rid of the java warning

            ExtCmdUtils.checkExitCode(
                output,
                StorageException::new,
                "LuksFormat failed"
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to initialize crypt device",
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Initializing dm-crypt device timed out",
                exc
            );
        }
        return getLuksVolumePath(identifier);
    }

    @Override
    public void openLuksDevice(String dev, String targetIdentifier, byte[] cryptKey) throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            // open cryptsetup
            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                null,
                CRYPTSETUP, "open", "--tries", "1", dev, CRYPT_PREFIX + targetIdentifier
            );
            outputStream.write(cryptKey);
            outputStream.write('\n');
            outputStream.flush();

            OutputData outputData = extCommand.syncProcess();
            outputStream.close(); // just to be sure and get rid of the java warning

            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to open dm-crypt device '" + CRYPT_PREFIX + targetIdentifier + "'"
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to initialize dm-crypt",
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Initializing dm-crypt device timed out",
                exc
            );
        }
    }

    @Override
    public void closeLuksDevice(String identifier) throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            OutputData outputData = extCommand.exec(CRYPTSETUP, "close", CRYPT_PREFIX + identifier);
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to close dm-crypt device '" + CRYPT_PREFIX + identifier + "'"
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to initialize dm-crypt",
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Initializing dm-crypt device timed out",
                exc
            );
        }
    }

    public void deleteHeaders(String backingDeviceRef) throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            OutputData outputData = extCommand.exec(
                "shred",
                "-s", "16M", // luks might only have 2M large headers, but we do not care. better safe than sorry :)
                "-z",
                backingDeviceRef
            );
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to erase LUKS headers from device '" + backingDeviceRef + "'"
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to initialize dm-crypt",
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Initializing dm-crypt device timed out",
                exc
            );
        }
    }


    public boolean hasLuksFormat(LuksVlmData<Resource> vlmData) throws StorageException
    {
        boolean hasLuks = false;

        ExtCmd extCmd = extCmdFactory.create();
        try
        {
            OutputData outputData = extCmd.exec(CRYPTSETUP, "isLuks", vlmData.getDataDevice());

            hasLuks = outputData.exitCode == 0;
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Check if device is already luks-formatted timed out",
                exc
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to check if device is already luks-formatted",
                exc
            );
        }

        return hasLuks;
    }

    public boolean isOpen(String identifier) throws StorageException
    {
        boolean open = false;
        ExtCmd extCmd = extCmdFactory.create();
        try
        {
            OutputData outputData = extCmd.exec("dmsetup", "ls", "--target", "crypt");
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to list dm-crypt devices"
            );

            // just to make sure that "foo" does not match "foobar"
            Pattern pattern = Pattern.compile(
                "^" + Pattern.quote(CRYPT_PREFIX + identifier) + "\\s+",
                Pattern.MULTILINE
            );

            String stdOut = new String(outputData.stdoutData);
            Matcher matcher = pattern.matcher(stdOut.trim());
            open = matcher.find();
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Listing open dm-crypt devices timed out",
                exc
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to list dm-crypt devices",
                exc
            );
        }
        return open;
    }

    public void grow(LuksVlmData<Resource> vlmDataRef, byte[] passphrase) throws StorageException
    {
        resize(vlmDataRef, null, passphrase);
    }

    public void shrink(LuksVlmData<Resource> vlmDataRef, byte[] passphrase) throws StorageException
    {
        resize(
            vlmDataRef,
            vlmDataRef.getUsableSize() * 2, // usableSize is in KiB, cryptsetup needs size in 512 byte-sectors
            passphrase
        );
    }

    public void changeKey(String dev, byte[] oldPassphrase, byte[] newPassphrase) throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            ArrayList<String> command = new ArrayList<>();
            command.add(CRYPTSETUP);
            command.add("luksChangeKey");
            if (version.greaterOrEqual(V2_0_0))
            {
                command.add("--pbkdf-memory");
                command.add(PBDKF_MAX_MEMORY_KIB); // in kib
            }
            command.add("--tries");
            command.add("1");
            command.add(dev);
            command.add("-S");
            command.add("0");

            // open cryptsetup
            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                null,
                command.toArray(new String[0])
            );
            outputStream.write(oldPassphrase);
            outputStream.write('\n');
            outputStream.write(newPassphrase);
            outputStream.write('\n');
            outputStream.write(newPassphrase);
            outputStream.write('\n');
            outputStream.flush();

            OutputData outputData = extCommand.syncProcess();
            outputStream.close(); // just to be sure and get rid of the java warning

            errorReporter.logInfo("cryptsetup changed key for " + dev);
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to change key on dm-crypt device '" + dev + "'"
            );
        }
        catch (IOException ioExc)
        {
            throw new StorageException(
                "Failed to change key on " + dev,
                ioExc
            );
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "cryptsetup change key on " + dev + " timed out",
                exc
            );
        }
    }

    private void resize(LuksVlmData<Resource> vlmDataRef, Long sizeRef, byte[] passphraseRef) throws StorageException
    {
        ExtCmd extCmd = extCmdFactory.create();
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            List<String> cmd = new ArrayList<>();
            cmd.add(CRYPTSETUP);
            cmd.add("resize");
            if (sizeRef != null)
            {
                cmd.add("--size");
                cmd.add(Long.toString(sizeRef));
            }
            cmd.add(CRYPT_PREFIX + vlmDataRef.getIdentifier());

            String[] cmdArr = cmd.toArray(new String[0]);

            if (version.greaterOrEqual(V2_1_0))
            {
                OutputStream outputStream = extCommand.exec(
                    ProcessBuilder.Redirect.PIPE,
                    null,
                    cmdArr
                );
                outputStream.write(passphraseRef);
                outputStream.write('\n');
                outputStream.flush();

                OutputData output = extCommand.syncProcess();
                outputStream.close(); // just to be sure and get rid of the java warning

                ExtCmdUtils.<StorageException>checkExitCode(
                    output,
                    StorageException::new,
                    "Luks resize failed"
                );
            }
            else
            {
                extCmd.exec(cmdArr);
            }
        }
        catch (ChildProcessTimeoutException exc)
        {
            throw new StorageException(
                "Resizing crypt-device timed out",
                exc
            );
        }
        catch (IOException exc)
        {
            throw new StorageException(
                "Failed to resize crypt devices",
                exc
            );
        }
    }

    @Override
    public String getLuksVolumePath(String identifier)
    {
        return "/dev/mapper/" + CRYPT_PREFIX + identifier;
    }
}
