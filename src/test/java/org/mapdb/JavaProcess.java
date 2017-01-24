package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class JavaProcess {

    private JavaProcess() {}

    public static Process exec(Class klass, String[] args) throws IOException,
            InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getName();
        List<String> commandArgs = new ArrayList<>();
        commandArgs.add(javaBin);
        commandArgs.add("-cp");
        commandArgs.add(classpath);
        commandArgs.add(className);
        commandArgs.addAll(Arrays.asList(args));

        ProcessBuilder builder = new ProcessBuilder(commandArgs);

        return builder.start();
    }
}
