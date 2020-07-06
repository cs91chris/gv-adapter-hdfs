/*******************************************************************************
 * Copyright (c) 2009, 2016 GreenVulcano ESB Open Source Project.
 * All rights reserved.
 *
 * This file is part of GreenVulcano ESB.
 *
 * GreenVulcano ESB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GreenVulcano ESB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with GreenVulcano ESB. If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/

package it.greenvulcano.gvesb.adapter.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.gvesb.adapter.hdfs.operations.BaseOperation;
import it.greenvulcano.gvesb.buffer.GVBuffer;
import it.greenvulcano.gvesb.virtual.CallException;
import it.greenvulcano.gvesb.virtual.CallOperation;
import it.greenvulcano.gvesb.virtual.ConnectionException;
import it.greenvulcano.gvesb.virtual.InitializationException;
import it.greenvulcano.gvesb.virtual.InvalidDataException;
import it.greenvulcano.gvesb.virtual.OperationKey;
import it.greenvulcano.util.metadata.PropertiesHandler;

public class HdfsCallOperation implements CallOperation {
	private OperationKey key = null;
	private static final String operationsPackage = "it.greenvulcano.gvesb.adapter.hdfs.operations";
	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HdfsCallOperation.class);
	
	protected String workDir = null;
	protected String endpoint = null;
	protected NodeList operations = null;
	protected List<BaseOperation> operationInstances = new ArrayList<>();
	
	private HdfsAPI hdfs = null;
	
	@Override
	public void init(Node node) throws InitializationException {
		try {
			operations = XMLConfig.getNodeList(node, "*[@type='hdfsOperation']");
			endpoint = PropertiesHandler.expand(XMLConfig.get(node, "@endpoint"));
			workDir = PropertiesHandler.expand(XMLConfig.get(node, "@working-directory"));
			
			if (endpoint == null) {
				throw new InitializationException("endopoint argument can not be null");
			}
			
			hdfs = new HdfsAPI(endpoint);
			
			for (int i = 0; i < operations.getLength(); i++) {
				BaseOperation op = null;
				Node opNode = operations.item(i);
				String className = XMLConfig.get(opNode, "@class");
				
				if (className == null || className == "") {
					className = operationsPackage + "." + opNode.getNodeName();
				}
				
				op = (BaseOperation) Class.forName(className).newInstance();
				op.setClient(hdfs);
				operationInstances.add(op);
				op.init(node);
			}
		}
		catch (Exception exc) {
			String[][] messages = new String[][] { { "message", exc.getMessage() } };
			throw new InitializationException("GV_INIT_SERVICE_ERROR", messages, exc);
		}
	}
	
	@Override
	public GVBuffer perform(GVBuffer gvBuffer) throws ConnectionException, CallException, InvalidDataException {
		try {
			if (workDir != null && workDir != "") {
				hdfs.setWD(workDir);
			}
			
			for (BaseOperation op : operationInstances) {
				gvBuffer = op.perform(gvBuffer);
			}
		}
		catch (Exception exc) {
			String[][] messages = new String[][] { { "service", gvBuffer.getService() },
					{ "system", gvBuffer.getSystem() }, { "tid", gvBuffer.getId().toString() },
					{ "message", exc.getMessage() } };
			throw new CallException("GV_CALL_SERVICE_ERROR", messages, exc);
		}
		return gvBuffer;
	}
	
	@Override
	public void cleanUp() {
		try {
			hdfs.close();
		}
		catch (IOException e) {
			logger.warn(e.getMessage());
		}
		
		for (BaseOperation op : operationInstances) {
			try {
				op.cleanUp();
			}
			catch (IOException e) {
				logger.debug(e.getMessage());
			}
		}
	}
	
	@Override
	public void destroy() {
		
	}
	
	@Override
	public String getServiceAlias(GVBuffer gvBuffer) {
		return gvBuffer.getService();
	}
	
	@Override
	public void setKey(OperationKey key) {
		this.key = key;
	}
	
	@Override
	public OperationKey getKey() {
		return key;
	}
}
