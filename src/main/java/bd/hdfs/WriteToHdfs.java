package bd.hdfs;

import java.net.URI;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WriteToHdfs {
	
	private static final Logger logger=Logger.getLogger("bd.hdfs.WriteToHdfs");
	
	public static void main(String[] args) throws Exception {
		
		String hdfsuri="hdfs://192.168.1.113";
		String path="/user/test/";
		String fileName="write_to_hdfs.txt";
		String fileContent="blablabla";
		
		Configuration conf=new Configuration(); 
		conf.set("fs.defaultFS", hdfsuri);
		
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		FileSystem fs=FileSystem.get(URI.create(hdfsuri), conf);
		
		Path newFolderPath=new Path(path);
		
		if (!fs.exists(newFolderPath)) {
			fs.mkdirs(newFolderPath);
			logger.info("Path "+path+" created.");
		}
		
		logger.info("Begin Write file into hdfs");
	    //Create a path
	    Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
	    //Init output stream
	    FSDataOutputStream outputStream=fs.create(hdfswritepath);
	    //Cassical output stream usage
//	    outputStream.writeBytes(fileContent);
//	    
//	    outputStream.close();
	    fs.copyFromLocalFile(new Path("words.txt"), hdfswritepath);
        logger.info("END OF COPY");
	    logger.info("End Write file into hdfs");
	}

}
