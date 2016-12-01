package com.github.kaeluka.spencer.server;

import com.github.kaeluka.spencer.instrumentation.ClassHierarchy;
import com.github.kaeluka.spencer.instrumentation.Instrument;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

public class TransformerServer {
    private static ServerSocket ss = null;
    private static volatile boolean tearDown = false;
    private static volatile Semaphore running = new Semaphore(0);
    private static final PrintStream out = null;
    private static ClassHierarchy hierarchy = new ClassHierarchy();

    static {
        try {
            FileUtils.deleteDirectory(new File("log/input/"));
            FileUtils.deleteDirectory(new File("log/output/"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void println(final PrintStream out, final String msg) {
        if (out != null) {
            out.println(msg);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException {
        new TransformerServer();
    }

    public static void tearDown() {
        TransformerServer.tearDown = true;
    }

    public static void awaitRunning() {
        try {
            TransformerServer.running.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void dumpClassDataToFile(byte[] recvd, String subdir) throws IOException {

        final String className = Instrument.getClassName(recvd);
        final String dumpFileName = "log/"+subdir+"/"+className+".class";

        final File classDumpFile = new File(dumpFileName);

//		println(this.out, "dumping class data to file "+classDumpFile.getAbsolutePath());

        if (!classDumpFile.getParentFile().exists()) {
            classDumpFile.getParentFile().mkdirs();
        }
        if (!classDumpFile.exists()) {
            classDumpFile.createNewFile();
        }
        FileOutputStream classDumpStream = new FileOutputStream(classDumpFile);
        classDumpStream.write(recvd);
        classDumpStream.close();
    }

    private static void sendByteArray(byte[] data) throws IOException {
        Socket socket = TransformerServer.ss.accept();
        DataOutputStream outstream = new DataOutputStream(socket.getOutputStream());
        //println(this.out, "length of new class is "+data.length);
        outstream.writeLong(data.length);
        //println(this.out, "wrote length, sending data");
        outstream.write(data);
        outstream.flush();
        outstream.close();
        //println(this.out, "sent data");
    }

    private static byte[] receiveByteArray() throws IOException {
        Socket socket = TransformerServer.ss.accept();
//		println(this.out, "Accepted connection");
        DataInputStream instream = new DataInputStream(socket.getInputStream());
        long len = readInt32(instream);
//		println(this.out, "length of original class is "+len);
        byte[] msgarr = new byte[(int)len];

        int actualLen = 0;
        do {
            final int readLen = instream.read(msgarr, actualLen, (int)(len-actualLen));
            if (readLen > 0) {
                actualLen += readLen;
//				println(this.out, "..."+actualLen);
            }
        } while (actualLen != len);

        return msgarr;
    }

    private static long readInt32(DataInputStream instream) throws IOException {
        long len = 0;
        for (int i=0; i<4; ++i) {
            int by = instream.readUnsignedByte();
            assert(by >= 0);
            len += by << (i*8);
//			println(this.out, by+" "+len);
        }
        return len;
    }

    private static void setupConnection() throws IOException {
        TransformerServer.ss = new ServerSocket(1345);
        TransformerServer.ss.setSoTimeout(1000);
    }

//	private static void closeConnection() throws IOException {
//		TransformerServer.ss.close();
//	}

    public TransformerServer() {
        try {
            setupConnection();
            TransformerServer.running.release();
            for(;;) {
                if (TransformerServer.tearDown) {
                    println(this.out, "stopping transformer server");
                    System.exit(0);
                }
//                println(this.out, "Listening for connection from instrumentation agent.. ");
                byte[] recvd = null;
                try {
                    try {
                        recvd = receiveByteArray();
                    } catch (SocketTimeoutException ex) {
                        //"coming up for air", the socket times out, so the
                        // server can check whether it has been killed
                        continue;
                    }
                    println(this.out, "received class "+Instrument.getClassName(recvd));
                    dumpClassDataToFile(recvd, "input");

                    byte[] transformed = recvd;
//                    try {
                        transformed = Instrument.transform(recvd,
                                out,
                                TransformerServer.hierarchy);
//                    } catch (Exception ex) {
//                        ex.printStackTrace(TransformerServer.out);
//                    }
                    if (! Arrays.equals(recvd, transformed)) {
                        println(TransformerServer.out, "transformed class");
                        dumpClassDataToFile(transformed, "output");
                    } else {
                        println(TransformerServer.out, "skipping class");
                    }
                    sendByteArray(transformed);

                } catch (Exception ex) {
                    ex.printStackTrace();
                    File errorLog = new File("log/"+Instrument.getClassName(recvd)+".error");
                    if (!errorLog.exists()) {
                        errorLog.getParentFile().mkdirs();
                        assert errorLog.createNewFile();
                    }
                    FileOutputStream errorStream = new FileOutputStream(errorLog);
                    if (ex.getMessage()!=null) {
                        errorStream.write(ex.getMessage().getBytes());
                    }
                    errorStream.write(ex.toString().getBytes());
                    errorStream.flush();
                    errorStream.close();
                    System.err.println("wrote error to <instrumentation_error.log>");
                    //Send the uninstrumented classfile back to the instrumentation tool (it's the best we can do...):
                    sendByteArray(recvd);
//					System.exit(1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}