package com.linbit.linstor.storage.utils;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

import java.io.IOException;
import java.io.OutputStream;

public class CryptSetup implements Crypt
{
    private static final String CRYPTSETUP = "cryptsetup";

    private Timer<String, Action<String>> timer;
    private ErrorReporter errorReporter;

    public CryptSetup(Timer<String, Action<String>> timerRef, ErrorReporter errorReporterRef)
    {
        timer = timerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public String createCryptDevice(
        String dev,
        byte[] cryptKey,
        OutputDataVerifier outVerifier,
        String identifier
    )
        throws StorageException
    {
        try
        {
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);

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

            outVerifier.verifyOutput(output, command);
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
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);

            // open cryptsetup
            OutputStream outputStream = extCommand.exec(
                ProcessBuilder.Redirect.PIPE,
                CRYPTSETUP, "luksOpen", dev, CRYPT_PREFIX + targetIdentifier
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
            final ExtCmd extCommand = new ExtCmd(timer, errorReporter);

            extCommand.exec(CRYPTSETUP, "close", CRYPT_PREFIX + identifier);
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
    public String getCryptVolumePath(String identifier)
    {
        return "/dev/mapper/" + CRYPT_PREFIX + identifier;
    }
}