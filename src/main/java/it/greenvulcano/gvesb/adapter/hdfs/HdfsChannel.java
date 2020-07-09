package it.greenvulcano.gvesb.adapter.hdfs;

import java.util.Optional;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.configuration.XMLConfigException;
import it.greenvulcano.gvesb.core.config.GreenVulcanoConfig;
import it.greenvulcano.util.metadata.PropertiesHandler;
import it.greenvulcano.util.xml.XMLUtils;

public class HdfsChannel {
    
    private final static Logger LOG = LoggerFactory.getLogger(HdfsChannel.class);    
    private final static ConcurrentMap<String, HdfsAPI> hdfsClients = new ConcurrentHashMap<>();
    
    public static void setup() {

        try {

            NodeList channelList = XMLConfig.getNodeList(GreenVulcanoConfig.getSystemsConfigFileName(), "//Channel[@type='HDFSAdapter']");

            LOG.debug("Enabled HDFSAdapter channels found: " + channelList.getLength());
            IntStream.range(0, channelList.getLength())
                     .mapToObj(channelList::item)
                     .map(HdfsChannel::getChannelEndpoint)
                     .filter(Optional::isPresent)
                     .map(Optional::get)
                     .forEach(HdfsChannel::buildHdfsAPI);

        } catch (XMLConfigException e) {
            LOG.error("Error reading configuration", e);
        }
    }

    public static void shutdown() {

        for (Entry<String, HdfsAPI> client : hdfsClients.entrySet()) {

            try {
                client.getValue().close();
            } catch (Exception e) {
                LOG.error("Error closing client for Channel " + client.getKey(), e);
            }
        }

        hdfsClients.clear();

    }
    
    public static Optional<HdfsAPI> getClient(Node callOperationNode) {
        try {
            
            String key =  PropertiesHandler.expand(XMLUtils.get_S(callOperationNode.getParentNode(), "@endpoint"));
            
            return Optional.of(hdfsClients.get(key));
        } catch (Exception e) {
            LOG.error("Error reading call-operation node configuration", e);
        }
        
        return Optional.empty();
    }
    
    private static Optional<String> getChannelEndpoint(Node channelNode) {

        try {

            if (XMLConfig.exists(channelNode, "@endpoint") && XMLConfig.getBoolean(channelNode, "@enabled", true)) {

                LOG.info("Configuring HDFSClient instance for Channel " + XMLUtils.get_S(channelNode, "@id-channel") + " in System"
                          + XMLUtils.get_S(channelNode.getParentNode(), "@id-system"));

                String uri = PropertiesHandler.expand(XMLUtils.get_S(channelNode, "@endpoint"));
                LOG.debug("HDFS URI: "+uri);
                return Optional.of(uri);

            }

        } catch (Exception e) {
            LOG.error("Error configuring HDFSClient", e);
        }
        
        return Optional.empty();

    }
    
    private static void buildHdfsAPI(String hdfsURI) {
        
        try {
            
            if (!hdfsClients.containsKey(hdfsURI)) {
                hdfsClients.put(hdfsURI, new HdfsAPI(hdfsURI));   
            }
            
        } catch (IOException e) {
            LOG.error("Error creating HDFSClient instance", e);
        }
        
    }
    

}
