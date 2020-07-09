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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import it.greenvulcano.configuration.XMLConfig;
import it.greenvulcano.gvesb.adapter.hdfs.operations.BaseOperation;
import it.greenvulcano.gvesb.adapter.hdfs.operations.ContentSummary;
import it.greenvulcano.gvesb.adapter.hdfs.operations.CreateDirs;
import it.greenvulcano.gvesb.adapter.hdfs.operations.Delete;
import it.greenvulcano.gvesb.adapter.hdfs.operations.GetFile;
import it.greenvulcano.gvesb.adapter.hdfs.operations.Move;
import it.greenvulcano.gvesb.adapter.hdfs.operations.PutFile;
import it.greenvulcano.gvesb.adapter.hdfs.operations.ReadFile;
import it.greenvulcano.gvesb.adapter.hdfs.operations.WriteFile;
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
	private static final Logger logger = org.slf4j.LoggerFactory.getLogger(HdfsCallOperation.class);
	
	protected String workDir = null;
	
	protected List<BaseOperation> operationInstances = new ArrayList<>();
	
	private HdfsAPI hdfs = null;
	
	private static final Map<String, Supplier<BaseOperation>> operationSuppliers;
	
	static {
	    operationSuppliers = new LinkedHashMap<>();	    
	    operationSuppliers.put(ContentSummary.class.getSimpleName(), ContentSummary::new);
	    operationSuppliers.put(CreateDirs.class.getSimpleName(), CreateDirs::new);
	    operationSuppliers.put(Delete.class.getSimpleName(), Delete::new);
	    operationSuppliers.put(GetFile.class.getSimpleName(), GetFile::new);
	    operationSuppliers.put(Move.class.getSimpleName(), Move::new);
	    operationSuppliers.put(PutFile.class.getSimpleName(), PutFile::new);
	    operationSuppliers.put(ReadFile.class.getSimpleName(), ReadFile::new);
	    operationSuppliers.put(WriteFile.class.getSimpleName(), WriteFile::new);
	    
	}	
	
	@Override
	public void init(Node node) throws InitializationException {
		try {
			
			workDir = PropertiesHandler.expand(XMLConfig.get(node, "@working-directory"));
			hdfs = HdfsChannel.getClient(node).orElseThrow(()-> new NoSuchElementException("HDFSClient not found"));
			
			NodeList configuredOperarions = XMLConfig.getNodeList(node, "*[@type='hdfsOperation']");
			
			for (int i = 0; i < configuredOperarions.getLength(); i++) {
			        Node operationNode = configuredOperarions.item(i);
				BaseOperation op = Optional.ofNullable(operationSuppliers.get(operationNode.getNodeName()))
				                           .map(Supplier::get)				                           
				                           .orElseThrow(()-> new NoSuchElementException("Invalid opereration: "+operationNode.getNodeName()));
				op.setClient(hdfs);				
				op.init(operationNode);
				operationInstances.add(op);	
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
