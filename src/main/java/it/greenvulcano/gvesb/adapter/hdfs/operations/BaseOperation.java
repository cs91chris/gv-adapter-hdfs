
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;

import org.slf4j.Logger;
import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.adapter.hdfs.HdfsAPI;
import it.greenvulcano.gvesb.adapter.hdfs.HdfsCallOperation;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class BaseOperation {
	protected static final Logger logger = org.slf4j.LoggerFactory.getLogger(HdfsCallOperation.class);
	
	protected HdfsAPI hdfs = null;
	private String workDir = null;
	
	public void setClient(HdfsAPI hdfs) {
		this.hdfs = hdfs;
	}
	
	public void init(Node node) throws XMLConfigException {
		String endpoint = XMLConfig.get(node, "@endpoint");
		if (endpoint != null && endpoint != "") {
			try {
				hdfs = new HdfsAPI(endpoint);
			}
			catch (IOException e) {
				throw new XMLConfigException(e.getMessage(), e);
			}
		}
		workDir = XMLConfig.get(node, "@working-directory");
	}
	
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		if (workDir != null && workDir != "") {
			hdfs.setWD(PropertiesHandler.expand(workDir, gvBuffer));
		}
		return gvBuffer;
	}
	
	public void cleanUp() throws IOException {
		hdfs.close();
	}
}
