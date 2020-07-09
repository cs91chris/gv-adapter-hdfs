
package it.greenvulcano.gvesb.adapter.hdfs.operations;

import java.io.IOException;
import java.util.Map;

import org.json.JSONObject;
import org.w3c.dom.Node;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.buffer.GVException;
import it.greenvulcano.gvesb.virtual.VCLException;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.metadata.PropertiesHandlerException;

public class ContentSummary extends BaseOperation {
	private String src = null;
	private boolean asJson = false;
	
	@Override
	public void init(Node node) throws XMLConfigException {
		super.init(node);
		
		src = XMLConfig.get(node, "@pathname");
		asJson = XMLConfig.getBoolean(node, "@as-json", true);
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws PropertiesHandlerException, VCLException {
		super.perform(gvBuffer);
		
		Map<String, String> summary;
		
		try {
			summary = hdfs.contentSummary(PropertiesHandler.expand(src, gvBuffer));
		}
		catch (IllegalArgumentException | IOException e) {
			throw new VCLException(e.getMessage(), e);
		}
		
		try {
			if (asJson == true) {
				JSONObject json = new JSONObject(summary);
				gvBuffer.setObject(json.toString());
			}
			else {
				for (String key : summary.keySet()) {
					gvBuffer.setProperty("HDFS_" + key.toUpperCase(), summary.get(key));
				}
			}
		}
		catch (GVException e) {
			throw new VCLException(e.getMessage(), e);
		}
		
		return gvBuffer;
	}
}
