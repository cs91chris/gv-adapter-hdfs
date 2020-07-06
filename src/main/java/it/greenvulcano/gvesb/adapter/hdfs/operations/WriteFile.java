
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.virtual.InvalidDataException;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class WriteFile extends BaseOperation {
	private String file = null;
	private boolean append = false;
	private boolean overwrite = false;
	
	@Override
	public void init(Node node) throws XMLConfigException {
		super.init(node);
		
		file = XMLConfig.get(node, "@file");
		append = XMLConfig.getBoolean(node, "@append");
		overwrite = XMLConfig.getBoolean(node, "@overwrite");
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		super.perform(gvBuffer);
		
		byte[] d;
		Object data = gvBuffer.getObject();
		file = PropertiesHandler.expand(file);
		
		if (data == null) {
			throw new InvalidDataException("The GVBuffer content is NULL");
		}
		
		if (data instanceof byte[]) {
			d = (byte[]) data;
		}
		else if (data instanceof String) {
			d = ((String) data).getBytes();
		}
		else {
			throw new InvalidDataException("Invalid GVBuffer content: " + data.getClass().getName());
		}
		
		ByteBuffer buff = ByteBuffer.allocate(d.length);
		buff.put(d);
		
		try {
			hdfs.writeFile(file, overwrite, append, buff);
		}
		catch (IOException e) {
			throw new VCLException(e.getMessage(), e);
		}
		return gvBuffer;
	}
}
