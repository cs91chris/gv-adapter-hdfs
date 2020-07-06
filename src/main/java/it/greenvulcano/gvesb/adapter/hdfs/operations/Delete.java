
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;

import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class Delete extends BaseOperation {
	private String pathname = null;
	private boolean recursive = false;
	
	@Override
	public void init(Node node) throws XMLConfigException {
		super.init(node);
		
		pathname = XMLConfig.get(node, "@pathname");
		recursive = XMLConfig.getBoolean(node, "@recursive");
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		super.perform(gvBuffer);
		
		try {
			hdfs.delete(PropertiesHandler.expand(pathname, gvBuffer), recursive);
		}
		catch (IllegalArgumentException | IOException e) {
			throw new VCLException(e.getMessage(), e);
		}
		return gvBuffer;
	}
}
