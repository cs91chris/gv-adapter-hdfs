
package it.greenvulcano.gvesb.adapter.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsAPI {
	private FileSystem hdfs;
	
	public HdfsAPI(String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.setClassLoader(DistributedFileSystem.class.getClassLoader());
		conf.setClass("fs.hdfs.impl", DistributedFileSystem.class, FileSystem.class);
		conf.setClass("fs.file.impl", LocalFileSystem.class, FileSystem.class);
		conf.set("fs.defaultFS", DistributedFileSystem.DEFAULT_FS);
		
		try {
			hdfs = FileSystem.get(new URI(hdfsPath), conf);
		}
		catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}
	
	public HdfsAPI(String coreFile, String hdfsFile) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(coreFile));
		conf.addResource(new Path(hdfsFile));
		
		hdfs = FileSystem.get(conf);
	}
	
	public void setWD(String wd) {
		if (wd != null) {
			hdfs.setWorkingDirectory(new Path(wd));
		}
	}
	
	public void close() throws IOException {
		hdfs.close();
	}
	
	public void delete(String file, boolean recursive) throws IllegalArgumentException, IOException {
		hdfs.delete(new Path(file), recursive);
	}
	
	public void move(String src, String dest) throws IllegalArgumentException, IOException {
		hdfs.rename(new Path(src), new Path(dest));
	}
	
	public void put(String src, String dest, boolean delSrc, boolean overwrite)
			throws IllegalArgumentException, IOException {
		hdfs.copyFromLocalFile(delSrc, overwrite, new Path(src), new Path(dest));
	}
	
	public void get(String src, String dest, boolean delSrc) throws IllegalArgumentException, IOException {
		hdfs.copyToLocalFile(delSrc, new Path(src), new Path(dest));
	}
	
	public void createDirs(String src) throws IllegalArgumentException, IOException {
		hdfs.mkdirs(new Path(src));
	}
	
	public void createDirs(String src, short permission) throws IllegalArgumentException, IOException {
		hdfs.mkdirs(new Path(src), FsPermission.createImmutable(permission));
	}
	
	public void writeFile(String filepath, boolean overwrite, boolean append, ByteBuffer data) throws IOException {
		FSDataOutputStream hdfsOutput = (append == true) ? hdfs.append(new Path(filepath))
				: hdfs.create(new Path(filepath), overwrite);
		
		OutputStreamWriter writer = new OutputStreamWriter(hdfsOutput, StandardCharsets.UTF_8);
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		
		CharBuffer buff = data.asCharBuffer();
		bufferedWriter.write(buff.array());
		bufferedWriter.newLine();
		hdfsOutput.close();
		bufferedWriter.close();
	}
	
	public HashMap<String, String> contentSummary(String file) throws IllegalArgumentException, IOException {
		HashMap<String, String> summary = new HashMap<String, String>();
		ContentSummary content = hdfs.getContentSummary(new Path(file));
		summary.put("directoryCount", Long.toString(content.getDirectoryCount()));
		summary.put("fileCount", Long.toString(content.getFileCount()));
		summary.put("length", Long.toString(content.getLength()));
		summary.put("quota", Long.toString(content.getQuota()));
		summary.put("spaceConsumed", Long.toString(content.getSpaceConsumed()));
		summary.put("spaceQuota", Long.toString(content.getSpaceQuota()));
		return summary;
	}
	
	public ByteBuffer readFile(String file) throws IllegalArgumentException, IOException {
		FSDataInputStream hdfsInput = hdfs.open(new Path(file));
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInput, StandardCharsets.UTF_8));
		
		int d = 0;
		int length = (int) Integer.parseInt(contentSummary(file).get("lenght"));
		ByteBuffer data = ByteBuffer.allocate(length);
		
		while (d != -1) {
			d = bufferedReader.read();
			data.put((byte) d);
		}
		
		hdfsInput.close();
		bufferedReader.close();
		return data;
	}
}
