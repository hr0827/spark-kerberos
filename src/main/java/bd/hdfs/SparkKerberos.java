package bd.hdfs;

import java.io.IOException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkKerberos {

	public static void main(String[] args) throws IOException {
		
		SparkConf sparkConfiguration = new SparkConf();
		 sparkConfiguration.set("spark.hadoop.hadoop.security.authentication", "kerberos");
		 sparkConfiguration.set("spark.hadoop.hadoop.security.authorization", "true");
		 sparkConfiguration.set("spark.hadoop.dfs.namenode.kerberos.principal","hdfs-malekcluster@DOMAIN.LOCAL");
		 sparkConfiguration.set("spark.hadoop.yarn.resourcemanager.principal", "yarn-malekcluster@DOMAIN.LOCAL");
	      
		  UserGroupInformation.loginUserFromKeytab("hdfs-malekcluster@DOMAIN.LOCAL", 
	    		   "/home/malek/eclipse-workspace/hdfs/hdfs.headless.keytab");
	      
		  UserGroupInformation.loginUserFromKeytab("yarn-malekcluster@DOMAIN.LOCAL", 
	    		   "/home/malek/eclipse-workspace/hdfs/yarn.service.keytab");
		 SparkSession spark = SparkSession.builder()
			      .appName("example-spark-scala-read-and-write-from-hdfs")
			      .master("local[*]")
			      .config(sparkConfiguration)
			    //  .config("HADOOP_USER_NAME", "hdfs")
			      .getOrCreate();
	     
		 
		 Dataset<Row> df_parquet = spark.read().parquet("hdfs://malek-pc:8020/tmp/parquet_data") ;
		 
		 df_parquet.show();
		
	}
}
