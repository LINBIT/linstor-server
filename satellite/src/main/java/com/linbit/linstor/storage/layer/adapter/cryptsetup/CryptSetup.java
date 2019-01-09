package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdUtils;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.timer.CoreTimer;
import com.linbit.utils.RemoveAfterDevMgrRework;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class CryptSetup implements Crypt
{
    private static final String CRYPTSETUP = "cryptsetup";

    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;

    @Inject
    public CryptSetup(
        ExtCmdFactory extCmdFactoryRef,
        ErrorReporter errorReporterRef
    )
    {
        extCmdFactory = extCmdFactoryRef;
        errorReporter = errorReporterRef;
    }

    @RemoveAfterDevMgrRework
    public CryptSetup(
        CoreTimer timer,
        ErrorReporter errorReporterRef
    )
    {
        extCmdFactory = new ExtCmdFactory(timer, errorReporterRef);
        errorReporter = errorReporterRef;
    }

    @Override
    public String createCryptDevice(
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
        return getCryptVolumePath(identifier);
    }

    @Override
    public void openCryptDevice(String dev, String targetIdentifier, byte[] cryptKey) throws StorageException
    {
        try
        {
            final ExtCmd extCommand = extCmdFactory.create();

            // open cryptsetup
            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                CRYPTSETUP, "open", dev, CRYPT_PREFIX + targetIdentifier
            );
            outputStream.write(cryptKey);
            outputStream.write('\n');
            outputStream.flush();

            extCommand.syncProcess();
            outputStream.close(); // just to be sure and get rid of the java warning
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
    public void closeCryptDevice(String identifier) throws StorageException
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

    public boolean hasLuksFormat(CryptSetupVlmStltData vlmData) throws StorageException
    {
        boolean hasLuks = false;

        ExtCmd extCmd = extCmdFactory.create();
        try
        {
            OutputData outputData = extCmd.exec(CRYPTSETUP, "isLuks", vlmData.backingDevice);

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
                "^" + Pattern.quote(CRYPT_PREFIX + identifier) + "\\s+"
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
    public String getCryptVolumePath(String identifier)
    {
        return "/dev/mapper/" + CRYPT_PREFIX + identifier;
    }
}
