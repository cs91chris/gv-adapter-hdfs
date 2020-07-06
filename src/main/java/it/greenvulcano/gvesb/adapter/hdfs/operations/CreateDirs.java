
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;

import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class CreateDirs extends BaseOperation {
	private String path = null;
	private short permission = -1;
	
	@Override
	public void init(Node node) throws XMLConfigException {
		super.init(node);
		
		path = XMLConfig.get(node, "@path");
		permission = (short) XMLConfig.getInteger(node, "@permission", -1);
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		super.perform(gvBuffer);
		
		path = PropertiesHandler.expand(path, gvBuffer);
		
		try {
			if (permission != -1) {
				hdfs.createDirs(path);
			}
			else {
				hdfs.createDirs(path, permission);
			}
		}
		catch (IllegalArgumentException | IOException e) {
			throw new VCLException(e.getMessage(), e);
		}
		
		return gvBuffer;
	}
}
