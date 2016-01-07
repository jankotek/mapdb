package org.mapdb.benchmark;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Main class to start benchmarks
 */
public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        //cleanup
        File res = new File("res");
        res.mkdirs();
        for(File f:res.listFiles()){
            if(!f.delete())
                throw new RuntimeException("Cold not delete: "+f);
        }


//        for(String name: InMemorySpaceUsage.fabs.keySet()) {
//            run(InMemorySpaceUsage.class, null, InMemorySpaceUsage.memUsage, null, name);
//        }

        for(String name: InMemoryCreate.fabs.keySet()) {
            run(InMemoryCreate.class, InMemoryCreate.memUsage, InMemoryCreate.memUsage, null, name);
        }


        for(String name: InMemoryUpdate.fabs.keySet()) {
            run(InMemoryUpdate.class, InMemoryUpdate.memUsage, InMemoryUpdate.memUsage, null, name);
        }

        for(String name: InMemoryGet.fabs.keySet()) {
            run(InMemoryGet.class, InMemoryGet.memUsage, InMemoryGet.memUsage, null, name);
        }

        //Charts.main(args);
    }

    /**
     * Runs class in separate JVM and saves its output
     */
    static void run(Class clazz, Integer heapMemStart, Integer heapMemMax, Integer directMem, String arg) throws InterruptedException, IOException {
        System.out.print(clazz.getSimpleName() + " - " + arg);

        long start = System.currentTimeMillis();

        ProcessBuilder b = new ProcessBuilder(
                jvmExecutable(),
                "-classpath", System.getProperty("java.class.path"),
                heapMemStart==null?"-Da=1":"-Xms"+heapMemStart+"G",
                heapMemMax==null?"-Da=1":"-Xmx"+heapMemMax+"G",
                directMem==null?"-Da=1":"-XX:MaxDirectMemorySize="+directMem+"G",
                clazz.getName(),
                arg
        );

        File out = File.createTempFile("mapdbTest","out");
        b.redirectOutput(out);
        Process pr = b.start();
        pr.waitFor();
        Thread.sleep(100);// just in case

        start = (System.currentTimeMillis()-start);
        System.out.println(" ... DONE " + start / 1000 + " seconds");
        BU.resultAdd(clazz.getSimpleName() + "." + arg + ".runtime", "" + start);
        BufferedReader r = new BufferedReader(new FileReader(out));
        String last="0";
        for(String line=r.readLine(); line!=null;line=r.readLine()){
            last = line;
        }
        out.delete();
        BU.resultAdd(clazz.getSimpleName()+"."+arg+".out",last);
        BU.resultAdd(clazz.getSimpleName()+"."+arg+".err",BU.outStreamToString(pr.getErrorStream()));
    }

    static String jvmExecutable(){
        String exec = System.getProperty("os.name").startsWith("Win") ? "java.exe":"java";
        String javaHome = System.getProperty("java.home");
        if(javaHome==null ||"".equals(javaHome))
            return exec;
        return javaHome+ File.separator + "bin" + File.separator + exec;
    }

}
