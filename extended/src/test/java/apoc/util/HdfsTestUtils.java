package apoc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

public class HdfsTestUtils {

    private HdfsTestUtils() {}
	
	private static void setHadoopHomeWindows() {
        if (System.getProperty("os.name").startsWith("Windows")) {
            String windowsLibDir = getHadoopHome();
            System.setProperty("hadoop.home.dir", windowsLibDir);
            loadLibs(windowsLibDir);
        }
    }

	private static void loadLibs(String windowsLibDir) {
		System.load(new File(windowsLibDir + File.separator + "/bin/hadoop.dll").getAbsolutePath());
		System.load(new File(windowsLibDir + File.separator + "/bin/hdfs.dll").getAbsolutePath());
	}

    private static String getHadoopHome() {
        if(System.getProperty("HADOOP_HOME") != null) {
            return System.getProperty("HADOOP_HOME");
        } else {
            File windowsLibDir = new File("hadoop");
            return windowsLibDir.getAbsolutePath();
        }
    }

    private static int getFreePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
    
    public static MiniDFSCluster getLocalHDFSCluster() throws Exception {
    	setHadoopHomeWindows();
    	Configuration conf = new HdfsConfiguration();
    	conf.set("fs.defaultFS", "hdfs://localhost");
		File hdfsPath = new File(System.getProperty("user.dir") + File.separator + "hadoop" + File.separator + "hdfs");
        hdfsPath.setWritable(true);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsPath.getAbsolutePath());
		MiniDFSCluster miniDFSCluster = new MiniDFSCluster.Builder(conf)
                .nameNodePort(getFreePort())
//                .nameNodeHttpPort(12341)
                .numDataNodes(1)
                .storagesPerDatanode(2)
                .format(true)
                .racks(null)
                .build();
		miniDFSCluster.waitActive();
		return miniDFSCluster;
    }
}
