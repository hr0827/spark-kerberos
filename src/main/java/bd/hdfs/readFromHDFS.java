package bd.hdfs;

import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class readFromHDFS {
	
	private static final Logger logger = Logger.getLogger("bd.hdfs.readFromHDFS");

	public static void main(String[] args) throws Exception{


	      String hdfsuri="hdfs://malek-pc";
	      String path="/user";
	      String fileName="words.txt";
	      
	      // ====== Init HDFS File System Object
	      Configuration conf = new Configuration();
	      // Set FileSystem URI
	      conf.set("fs.defaultFS", hdfsuri);
	      conf.set("hadoop.security.authentication", "kerberos");
	      UserGroupInformation.setConfiguration(conf);
	     
	      UserGroupInformation.loginUserFromKeytab("hdfs-malekcluster@DOMAIN.LOCAL", 
	    		   "/home/malek/eclipse-workspace/hdfs/hdfs.headless.keytab");
	      
	      // Because of Maven
//	      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//	      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	      // Set HADOOP user
	      System.setProperty("HADOOP_USER_NAME", "hdfs");
	      System.setProperty("hadoop.home.dir", "/");
System.out.println( System.getProperty("HADOOP_USER_NAME"));
	      FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
	      FileStatus[] fsStatus = fs.listStatus(new Path("/"));
	      for(int i = 0; i < fsStatus.length; i++){
	         System.out.println(fsStatus[i].getPath().toString());
	      }
	      //==== Create folder if not exists
	      Path workingDir=new Path("test2.txt");
	      Path newFolderPath= new Path(path);

	      if(!fs.exists(newFolderPath)) {
	    	  
	         // Create new Directory
	         fs.mkdirs(newFolderPath);
	         logger.info("Path "+path+" created.");}
	         
	       //==== Read file
	         logger.info("Read file into hdfs");
	         //Create a path
//	         Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);
//	         //Init input stream
//	         FSDataInputStream inputStream = fs.open(hdfsreadpath);
//	         //Classical input stream usage
//	         String out= IOUtils.toString(inputStream, "UTF-8");
//	         logger.info(out);
//
//	         fs.copyFromLocalFile(new Path("words.txt"), hdfsreadpath);
//	         logger.info("END OF COPY");
//	        	 inputStream.close();
	        	 fs.close();

	         
	}	
}
