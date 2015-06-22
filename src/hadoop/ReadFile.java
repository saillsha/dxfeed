package hadoop;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

public class ReadFile {
	public static void main(String[] args){
		Configuration conf = new Configuration();
		/* Stack overflow comment:
		 * 
		 * When you have replication, you have to make sure that the replication
		 * factor is less than or equal to the number of your datanodes.
		 * The reason why writing for the first time succeeds and appending fails
		 * when the replication factor is greater than the number of datanodes is
		 * that appending is more strict to ensure consistency whereas writing for the first time can tolerate under-replication.
		 */
		conf.set("dfs.replication", "1");
		writeFile(conf);
	}
	
	static void writeFile(Configuration conf){
		try{
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
			Path file = new Path("hdfs://localhost:9000/scratch/testfile");
			fs.setReplication(file, (short) 1);
			OutputStream os;
			if(fs.exists(file)){
				os = fs.append(file, 4096);
			}
			else{
				os = fs.create(file);				
			}
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			br.write("This is a test file\n");
			br.write("don't worry about me");
			br.close();
			fs.close();
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally{
			
		}
	}
	
	static void readFile(Configuration conf){
		try{
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000/scratch/scratch"), conf);
			InputStream in = fs.open(new Path("hdfs://localhost:9000/scratch/scratch"));
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}
}
