package com.linbit.linstor.core;

/**
 *
 * @author rpeinthor
 */
public class ControllerArguments {
    private String working_directory;

    public ControllerArguments() {
        this.working_directory = "";
    }

    public void setWorkingDirectory(final String working_directory) {
        this.working_directory = working_directory;
    }

    public String getWorkingDirectory() {
        return this.working_directory;
    }
}
