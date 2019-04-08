package com.linbit.linstor.storage.layer.adapter.luks;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.utils.Luks;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class CryptSetupCommands implements Luks
{
    private static final String CRYPTSETUP = "cryptsetup";

    @SuppressWarnings("unused")
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public CryptSetupCommands(
        ExtCmdFactory extCmdFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;
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

            // init
            String[] command = new String[] {
                CRYPTSETUP,
                "-q",
                "luksFormat",
                dev
            };
            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                command
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
                CRYPTSETUP, "open", "--tries", "1", dev, LUKS_PREFIX + targetIdentifier
            );
            outputStream.write(cryptKey);
            outputStream.write('\n');
            outputStream.flush();

            OutputData outputData = extCommand.syncProcess();
            outputStream.close(); // just to be sure and get rid of the java warning

            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to open dm-crypt device" + LUKS_PREFIX + targetIdentifier + "'"
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

            OutputData outputData = extCommand.exec(CRYPTSETUP, "close", LUKS_PREFIX + identifier);
            ExtCmdUtils.checkExitCode(
                outputData,
                StorageException::new,
                "Failed to close dm-crypt device '" + LUKS_PREFIX + identifier + "'"
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

    public boolean hasLuksFormat(LuksVlmData vlmData) throws StorageException
    {
        boolean hasLuks = false;

        ExtCmd extCmd = extCmdFactory.create();
        try
        {
            OutputData outputData = extCmd.exec(CRYPTSETUP, "isLuks", vlmData.getBackingDevice());

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
                "^" + Pattern.quote(LUKS_PREFIX + identifier) + "\\s+"
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

    @Override
    public String getLuksVolumePath(String identifier)
    {
        return "/dev/mapper/" + LUKS_PREFIX + identifier;
    }
}
