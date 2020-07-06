
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;

import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.buffer.GVException;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class ReadFile extends BaseOperation {
	private String file = null;
	
	@Override
	public void init(Node node) throws XMLConfigException {
		super.init(node);
		
		file = XMLConfig.get(node, "@file");
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		super.perform(gvBuffer);
		
		file = PropertiesHandler.expand(file, gvBuffer);
		
		try {
			gvBuffer.setObject(hdfs.readFile(file).array());
		}
		catch (IllegalArgumentException | GVException | IOException e) {
			throw new VCLException(e.getMessage(), e);
		}
		return gvBuffer;
	}
}
