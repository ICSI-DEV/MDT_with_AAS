/*
 * Copyright (c) 2019 the Eclipse Milo Authors
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package plantpulse.plugin.opcua.server;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ubyte;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ulong;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.ushort;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.core.ValueRank;
import org.eclipse.milo.opcua.sdk.core.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.server.Lifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.DataTypeDictionaryManager;
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.BaseEventTypeNode;
import org.eclipse.milo.opcua.sdk.server.model.nodes.objects.ServerTypeNode;
import org.eclipse.milo.opcua.sdk.server.model.nodes.variables.AnalogItemTypeNode;
import org.eclipse.milo.opcua.sdk.server.nodes.AttributeContext;
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.AttributeDelegate;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.AttributeDelegateChain;
import org.eclipse.milo.opcua.sdk.server.nodes.factories.NodeFactory;
import org.eclipse.milo.opcua.sdk.server.nodes.filters.AttributeFilters;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.XmlElement;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.structured.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import plantpulse.json.JSONObject;
import plantpulse.plugin.opcua.cache.LastValueMap;
import plantpulse.plugin.opcua.cache.OPCUAInfoCacheFactory;
import plantpulse.plugin.opcua.cache.TagCacheFactory;

/**
 * PlantPulseNamespace
 * @author leesa
 *
 */
public class PlantPulseNamespace extends ManagedNamespaceWithLifecycle {
	
	private static final Log log = LogFactory.getLog(PlantPulseNamespace.class);
	
	public static final String ROOT_FOLDER_NAME =  "PLANTPULSE";
	public static final String NAMESPACE_URI    = "urn:plantpulse:opcua:server";
	
    private static final Object[][] STATIC_SCALAR_NODES = new Object[][]{
        {"Boolean", Identifiers.Boolean, new Variant(false)},
        {"Byte", Identifiers.Byte, new Variant(ubyte(0x00))},
        {"SByte", Identifiers.SByte, new Variant((byte) 0x00)},
        {"Integer", Identifiers.Integer, new Variant(32)},
        {"Int16", Identifiers.Int16, new Variant((short) 16)},
        {"Int32", Identifiers.Int32, new Variant(32)},
        {"Int64", Identifiers.Int64, new Variant(64L)},
        {"UInteger", Identifiers.UInteger, new Variant(uint(32))},
        {"UInt16", Identifiers.UInt16, new Variant(ushort(16))},
        {"UInt32", Identifiers.UInt32, new Variant(uint(32))},
        {"UInt64", Identifiers.UInt64, new Variant(ulong(64L))},
        {"Float", Identifiers.Float, new Variant(3.14f)},
        {"Double", Identifiers.Double, new Variant(3.14d)},
        {"String", Identifiers.String, new Variant("string value")},
        {"DateTime", Identifiers.DateTime, new Variant(DateTime.now())},
        {"Guid", Identifiers.Guid, new Variant(UUID.randomUUID())},
        {"ByteString", Identifiers.ByteString, new Variant(new ByteString(new byte[]{0x01, 0x02, 0x03, 0x04}))},
        {"XmlElement", Identifiers.XmlElement, new Variant(new XmlElement("<a>hello</a>"))},
        {"LocalizedText", Identifiers.LocalizedText, new Variant(LocalizedText.english("localized text"))},
        {"QualifiedName", Identifiers.QualifiedName, new Variant(new QualifiedName(1234, "defg"))},
        {"NodeId", Identifiers.NodeId, new Variant(new NodeId(1234, "abcd"))},
        {"Variant", Identifiers.BaseDataType, new Variant(32)},
        {"Duration", Identifiers.Duration, new Variant(1.0)},
        {"UtcTime", Identifiers.UtcTime, new Variant(DateTime.now())},
    };

    private static final Object[][] STATIC_ARRAY_NODES = new Object[][]{
        {"BooleanArray", Identifiers.Boolean, false},
        {"ByteArray", Identifiers.Byte, ubyte(0)},
        {"SByteArray", Identifiers.SByte, (byte) 0x00},
        {"Int16Array", Identifiers.Int16, (short) 16},
        {"Int32Array", Identifiers.Int32, 32},
        {"Int64Array", Identifiers.Int64, 64L},
        {"UInt16Array", Identifiers.UInt16, ushort(16)},
        {"UInt32Array", Identifiers.UInt32, uint(32)},
        {"UInt64Array", Identifiers.UInt64, ulong(64L)},
        {"FloatArray", Identifiers.Float, 3.14f},
        {"DoubleArray", Identifiers.Double, 3.14d},
        {"StringArray", Identifiers.String, "string value"},
        {"DateTimeArray", Identifiers.DateTime, DateTime.now()},
        {"GuidArray", Identifiers.Guid, UUID.randomUUID()},
        {"ByteStringArray", Identifiers.ByteString, new ByteString(new byte[]{0x01, 0x02, 0x03, 0x04})},
        {"XmlElementArray", Identifiers.XmlElement, new XmlElement("<a>hello</a>")},
        {"LocalizedTextArray", Identifiers.LocalizedText, LocalizedText.english("localized text")},
        {"QualifiedNameArray", Identifiers.QualifiedName, new QualifiedName(1234, "defg")},
        {"NodeIdArray", Identifiers.NodeId, new NodeId(1234, "abcd")}
    };


    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile Thread eventThread;
    private volatile boolean keepPostingEvents = true;

    private final Random random = new Random();

    private final DataTypeDictionaryManager dictionaryManager;

    private final SubscriptionModel subscriptionModel;

    PlantPulseNamespace(OpcUaServer server) {
        super(server, NAMESPACE_URI);

        
        subscriptionModel = new SubscriptionModel(server, this);
        dictionaryManager = new DataTypeDictionaryManager(getNodeContext(), NAMESPACE_URI);

        getLifecycleManager().addLifecycle(dictionaryManager);
        getLifecycleManager().addLifecycle(subscriptionModel);

        getLifecycleManager().addStartupTask(this::createAndAddNodes);

        getLifecycleManager().addLifecycle(new Lifecycle() {
            @Override
            public void startup() {
                startBogusEventNotifier();
            }

            @Override
            public void shutdown() {
                try {
                    keepPostingEvents = false;
                    eventThread.interrupt();
                    eventThread.join();
                } catch (InterruptedException ignored) {
                    // ignored
                }
            }
        });
    }

    private void createAndAddNodes() {
    	
    	/////////----------------------------------------------------------------------------------------------------
        // Create a "ROOT_FOLDER_NAME" folder and add it to the node manager
    	/////////----------------------------------------------------------------------------------------------------
        NodeId folderNodeId = newNodeId(ROOT_FOLDER_NAME);

        UaFolderNode folderNode = new UaFolderNode(
            getNodeContext(),
            folderNodeId,
            newQualifiedName(ROOT_FOLDER_NAME),
            LocalizedText.english(ROOT_FOLDER_NAME)
        );
        
        OPCUAInfoCacheFactory.getInstance().put("ROOT_FOLDER_NODE", folderNode);

        getNodeManager().addNode(folderNode);

        // Make sure our new folder shows up under the server's Objects folder.
        folderNode.addReference(new Reference(
            folderNode.getNodeId(),
            Identifiers.Organizes,
            Identifiers.ObjectsFolder.expanded(),
            false
        ));

        // Add the rest of the nodes
        addVariableNodes(folderNode);

        log.info("루트 폴더 및 기본 노드 생성이 완료되었습니다.");
        
    };
    
    
    public String addNode(JSONObject tag) {
    	//
    	log.debug("Regstring tag to OPC-UA node : " + tag.toString());

    	//
    	UaFolderNode rootNode = OPCUAInfoCacheFactory.getInstance().get("ROOT_FOLDER_NODE");
    	
    	//사이트 폴더 생성
    	String site_id = "";
    	String site_name = "";
    	String site_desc = "";
    	if(tag.containsKey("site_id")){
    		site_id = tag.getString("site_id");
    		site_name = tag.getString("site_name");
    		site_desc = tag.getString("site_desc");
    	}
    	
    	UaFolderNode siteFolder;
    	if(OPCUAInfoCacheFactory.getInstance().get(site_id) == null){
    		siteFolder = new UaFolderNode(
    				getNodeContext(),
    	            new NodeId(getNamespaceIndex(), ROOT_FOLDER_NAME + "." + site_id),
    	            new QualifiedName(getNamespaceIndex(), site_name),
    	            LocalizedText.english(site_name)
    	    );
            getNodeManager().addNode(siteFolder);


    	}else{
    		siteFolder = (UaFolderNode) OPCUAInfoCacheFactory.getInstance().get(site_id);
    	};
    	 rootNode.addOrganizes(siteFolder);
    	
    	//OPC 폴더 생성
    	String opc_id = "";
    	String opc_name = "";
    	String opc_desc = "";
    	if(tag.containsKey("opc_id")){
    		opc_id = tag.getString("opc_id");
    		opc_name = tag.getString("opc_name");
    		opc_desc = tag.getString("opc_desc");
    	}
    	
    	UaFolderNode opcFolder;
    	if(OPCUAInfoCacheFactory.getInstance().get(opc_id) == null){
    		opcFolder = new UaFolderNode(
    				getNodeContext(),
    	            new NodeId(getNamespaceIndex(),ROOT_FOLDER_NAME + "." + site_id + "." + opc_id),
    	            new QualifiedName(getNamespaceIndex(), opc_name),
    	            LocalizedText.english(opc_name)
    	    );
            getNodeManager().addNode(opcFolder);

    	}else{
    		opcFolder = (UaFolderNode) OPCUAInfoCacheFactory.getInstance().get(opc_id);
    	}
    
        this.addVariableNodes(opcFolder);
        siteFolder.addOrganizes(opcFolder);

        String tag_id = tag.getString("tag_id");
        String tag_name = tag.getString("tag_name");
    	String data_type = tag.getString("type");
    	String tag_desc = tag.getString("tag_desc");
    	//
    	NodeId node_data_type = null;
		try {
			node_data_type = getDataType(tag_id, data_type);
		} catch (Exception e) {
			log.error(e,e);
		}
    	
		String node_str = ROOT_FOLDER_NAME + "." + site_id + "." + opc_id + "." + tag_id;
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
            .setNodeId(new NodeId(getNamespaceIndex(), node_str))
            .setAccessLevel(AccessLevel.READ_ONLY)
            .setUserAccessLevel(AccessLevel.READ_ONLY)
            .setBrowseName(new QualifiedName(getNamespaceIndex(), tag_name))
            .setDisplayName(LocalizedText.english(tag_name))
            //.setDescription(LocalizedText.english(tag_desc))
            .setDataType(node_data_type)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();
        
     
        /*
        AttributeDelegate delegate = AttributeDelegateChain.create(
            new AttributeDelegate() {
                @Override
                public DataValue getValue(AttributeContext context, VariableNode node) throws UaException {
                   return LastValueMap.getInstance().get(tag_id);
                }
            }
        );
        node.setAttributeDelegate(delegate);
        */
        
        getNodeManager().addNode(node);

        opcFolder.addComponent(node);
        
        OPCUAInfoCacheFactory.getInstance().put(opc_id,opcFolder);
        
        log.debug("Regstring tag to OPC-UA node : namespace_index=[" + getNamespaceIndex() + "], id=[" + node_str +"]");
    	
        return node_str;
    }
    
    
    public NodeId getDataType(String tag_id, String data_type) throws  Exception {
    	NodeId typeId = null;
    	if("Double".equals(data_type)) {
   		 typeId = Identifiers.Double;
    	}else if("Float".equals(data_type)) {
      		 typeId = Identifiers.Float;
    	}else if("Integer".equals(data_type)) {
     		 typeId = Identifiers.Integer;
    	}else if("Long".equals(data_type)) {
    		 typeId = Identifiers.Integer;
    	}else if("Short".equals(data_type)) {
   		 typeId = Identifiers.Integer;
    	}else if("Date".equals(data_type)) {
   		 typeId = Identifiers.DateTime;
    	}else if("Boolean".equals(data_type)) {
    		 typeId = Identifiers.Boolean;
    	}else if("String".equals(data_type)) {
    		typeId = Identifiers.Boolean;
    	}else {
    		throw new Exception("Unsupport node data type : tag_id=[" + tag_id + "], type=[" +  data_type+ "]");
    	}
    	return typeId;
    }
    
    public UaVariableNode getNode(JSONObject tag) {
    	if(tag.get("tag_id") != null){
	    	if(!TagCacheFactory.getInstance().getMap().containsKey(tag.getString("tag_id"))){
	    		String node_str = ROOT_FOLDER_NAME + "." + tag.getString("site_id") + "." + tag.getString("opc_id") + "." + tag.getString("tag_id");
	    		NodeId tag_node = new NodeId(getNamespaceIndex(), node_str );
	    		return (UaVariableNode) this.getNodeManager().getNode(tag_node).get();
	    	}else {
	    		 return null;
	    	}
		}else {
		   return null;
		}
    }
    
    public Variant getVariant(String tag_id, String type , String value) throws Exception {
    	Variant var = null;
    	if("int".equals(type) || "float".equals(type) || "double".equals(type)  || "long".equals(type)) {
			var = new Variant(Double.parseDouble(value));
    	}else if("timestamp".equals(type) || "date".equals(type)) {
    		var = new Variant(new Date(Long.parseLong(value)));
    	}else if("boolean".equals(type)) {
    		var = new Variant(Boolean.parseBoolean(value));
    	}else if("string".equals(type)) {
    		var = new Variant((value));
    	}else {
    		throw new Exception("Unsupport variant type : tag_id=[" + tag_id + "], type=[" +  type+ "]");
    	}
    	return var;
    }
    
    public void setValue(JSONObject tag) throws Exception {
    	if(this.getNode(tag) != null){
			
			String tag_id = tag.getString("tag_id");
			String data_type = tag.getString("type");
			String value = tag.getString("value");
			Date   source_time  = new Date(tag.getLong("timestamp"));
			Date   server_time  = new Date();
			int quality = tag.getInt("quality");
			
			Variant var = getVariant( tag_id,  data_type ,  value);
			DataValue ua_value = new DataValue(var, (quality == 192) ? StatusCode.GOOD : StatusCode.BAD, new DateTime(source_time), new DateTime(server_time));
			this.getNode(tag).setValue(ua_value);
			//
			LastValueMap.getInstance().put(tag.getString("tag_id"), ua_value);
		}
    }

    private void startBogusEventNotifier() {
        // Set the EventNotifier bit on Server Node for Events.
        UaNode serverNode = getServer()
            .getAddressSpaceManager()
            .getManagedNode(Identifiers.Server)
            .orElse(null);

        if (serverNode instanceof ServerTypeNode) {
            ((ServerTypeNode) serverNode).setEventNotifier(ubyte(1));

            // Post a bogus Event every couple seconds
            eventThread = new Thread(() -> {
                while (keepPostingEvents) {
                    try {
                        BaseEventTypeNode eventNode = getServer().getEventFactory().createEvent(
                            newNodeId(UUID.randomUUID()),
                            Identifiers.BaseEventType
                        );

                        eventNode.setBrowseName(new QualifiedName(1, "foo"));
                        eventNode.setDisplayName(LocalizedText.english("foo"));
                        eventNode.setEventId(ByteString.of(new byte[]{0, 1, 2, 3}));
                        eventNode.setEventType(Identifiers.BaseEventType);
                        eventNode.setSourceNode(serverNode.getNodeId());
                        eventNode.setSourceName(serverNode.getDisplayName().getText());
                        eventNode.setTime(DateTime.now());
                        eventNode.setReceiveTime(DateTime.NULL_VALUE);
                        eventNode.setMessage(LocalizedText.english("event message!"));
                        eventNode.setSeverity(ushort(2));

                        //noinspection UnstableApiUsage
                        getServer().getEventBus().post(eventNode);

                        eventNode.delete();
                    } catch (Throwable e) {
                        logger.error("Error creating EventNode: {}", e.getMessage(), e);
                    }

                    try {
                        //noinspection BusyWait
                        Thread.sleep(2_000);
                    } catch (InterruptedException ignored) {
                        // ignored
                    }
                }
            }, "bogus-event-poster");

            eventThread.start();
        }
    }

    private void addVariableNodes(UaFolderNode rootNode) {
       /*
    	addArrayNodes(rootNode);
        addScalarNodes(rootNode);
        addAdminReadableNodes(rootNode);
        addAdminWritableNodes(rootNode);
        addDynamicNodes(rootNode);
        addDataAccessNodes(rootNode);
        addWriteOnlyNodes(rootNode);
        */
    }

    private void addArrayNodes(UaFolderNode rootNode) {
        UaFolderNode arrayTypesFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME+"/ArrayTypes"),
            newQualifiedName("ArrayTypes"),
            LocalizedText.english("ArrayTypes")
        );

        getNodeManager().addNode(arrayTypesFolder);
        rootNode.addOrganizes(arrayTypesFolder);

        for (Object[] os : STATIC_ARRAY_NODES) {
            String name = (String) os[0];
            NodeId typeId = (NodeId) os[1];
            Object value = os[2];
            Object array = Array.newInstance(value.getClass(), 5);
            for (int i = 0; i < 5; i++) {
                Array.set(array, i, value);
            }
            Variant variant = new Variant(array);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/ArrayTypes/" + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setUserAccessLevel(AccessLevel.READ_WRITE)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .setValueRank(ValueRank.OneDimension.getValue())
                .setArrayDimensions(new UInteger[]{uint(0)})
                .build();

            node.setValue(new DataValue(variant));

            node.getFilterChain().addLast(new AttributeLoggingFilter(AttributeId.Value::equals));

            getNodeManager().addNode(node);
            arrayTypesFolder.addOrganizes(node);
        }
    }

    private void addScalarNodes(UaFolderNode rootNode) {
        UaFolderNode scalarTypesFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME + "/ScalarTypes"),
            newQualifiedName("ScalarTypes"),
            LocalizedText.english("ScalarTypes")
        );

        getNodeManager().addNode(scalarTypesFolder);
        rootNode.addOrganizes(scalarTypesFolder);

        for (Object[] os : STATIC_SCALAR_NODES) {
            String name = (String) os[0];
            NodeId typeId = (NodeId) os[1];
            Variant variant = (Variant) os[2];

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(ROOT_FOLDER_NAME + "/ScalarTypes/" + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setUserAccessLevel(AccessLevel.READ_WRITE)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            node.getFilterChain().addLast(new AttributeLoggingFilter(AttributeId.Value::equals));

            getNodeManager().addNode(node);
            scalarTypesFolder.addOrganizes(node);
        }
    }

    private void addWriteOnlyNodes(UaFolderNode rootNode) {
        UaFolderNode writeOnlyFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME + "/WriteOnly"),
            newQualifiedName("WriteOnly"),
            LocalizedText.english("WriteOnly")
        );

        getNodeManager().addNode(writeOnlyFolder);
        rootNode.addOrganizes(writeOnlyFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
            .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/WriteOnly/" + name))
            .setAccessLevel(AccessLevel.WRITE_ONLY)
            .setUserAccessLevel(AccessLevel.WRITE_ONLY)
            .setBrowseName(newQualifiedName(name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("can't read this")));

        getNodeManager().addNode(node);
        writeOnlyFolder.addOrganizes(node);
    }

    private void addAdminReadableNodes(UaFolderNode rootNode) {
        UaFolderNode adminFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME+"/OnlyAdminCanRead"),
            newQualifiedName("OnlyAdminCanRead"),
            LocalizedText.english("OnlyAdminCanRead")
        );

        getNodeManager().addNode(adminFolder);
        rootNode.addOrganizes(adminFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
            .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/OnlyAdminCanRead/" + name))
            .setAccessLevel(AccessLevel.READ_WRITE)
            .setBrowseName(newQualifiedName(name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("shh... don't tell the lusers")));

        node.getFilterChain().addLast(new RestrictedAccessFilter(identity -> {
            if ("admin".equals(identity)) {
                return AccessLevel.READ_WRITE;
            } else {
                return AccessLevel.NONE;
            }
        }));

        getNodeManager().addNode(node);
        adminFolder.addOrganizes(node);
    }

    private void addAdminWritableNodes(UaFolderNode rootNode) {
        UaFolderNode adminFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME + "/OnlyAdminCanWrite"),
            newQualifiedName("OnlyAdminCanWrite"),
            LocalizedText.english("OnlyAdminCanWrite")
        );

        getNodeManager().addNode(adminFolder);
        rootNode.addOrganizes(adminFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
            .setNodeId(newNodeId(ROOT_FOLDER_NAME + "/OnlyAdminCanWrite/" + name))
            .setAccessLevel(AccessLevel.READ_WRITE)
            .setBrowseName(newQualifiedName(name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("admin was here")));

        node.getFilterChain().addLast(new RestrictedAccessFilter(identity -> {
            if ("admin".equals(identity)) {
                return AccessLevel.READ_WRITE;
            } else {
                return AccessLevel.READ_ONLY;
            }
        }));

        getNodeManager().addNode(node);
        adminFolder.addOrganizes(node);
    }

    private void addDynamicNodes(UaFolderNode rootNode) {
        UaFolderNode dynamicFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME+"/Dynamic"),
            newQualifiedName("Dynamic"),
            LocalizedText.english("Dynamic")
        );

        getNodeManager().addNode(dynamicFolder);
        rootNode.addOrganizes(dynamicFolder);

        // Dynamic Boolean
        {
            String name = "Boolean";
            NodeId typeId = Identifiers.Boolean;
            Variant variant = new Variant(false);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/Dynamic/" + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            node.getFilterChain().addLast(
                new AttributeLoggingFilter(),
                AttributeFilters.getValue(
                    ctx ->
                        new DataValue(new Variant(random.nextBoolean()))
                )
            );

            getNodeManager().addNode(node);
            dynamicFolder.addOrganizes(node);
        }

        // Dynamic Int32
        {
            String name = "Int32";
            NodeId typeId = Identifiers.Int32;
            Variant variant = new Variant(0);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/Dynamic/" + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            node.getFilterChain().addLast(
                new AttributeLoggingFilter(),
                AttributeFilters.getValue(
                    ctx ->
                        new DataValue(new Variant(random.nextInt()))
                )
            );

            getNodeManager().addNode(node);
            dynamicFolder.addOrganizes(node);
        }

        // Dynamic Double
        {
            String name = "Double";
            NodeId typeId = Identifiers.Double;
            Variant variant = new Variant(0.0);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(getNodeContext())
                .setNodeId(newNodeId(ROOT_FOLDER_NAME+"/Dynamic/" + name))
                .setAccessLevel(AccessLevel.READ_WRITE)
                .setBrowseName(newQualifiedName(name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            node.getFilterChain().addLast(
                new AttributeLoggingFilter(),
                AttributeFilters.getValue(
                    ctx ->
                        new DataValue(new Variant(random.nextDouble()))
                )
            );

            getNodeManager().addNode(node);
            dynamicFolder.addOrganizes(node);
        }
    }

    private void addDataAccessNodes(UaFolderNode rootNode) {
        // DataAccess folder
        UaFolderNode dataAccessFolder = new UaFolderNode(
            getNodeContext(),
            newNodeId(ROOT_FOLDER_NAME+"/DataAccess"),
            newQualifiedName("DataAccess"),
            LocalizedText.english("DataAccess")
        );

        getNodeManager().addNode(dataAccessFolder);
        rootNode.addOrganizes(dataAccessFolder);

        try {
            AnalogItemTypeNode node = (AnalogItemTypeNode) getNodeFactory().createNode(
                newNodeId(ROOT_FOLDER_NAME+"/DataAccess/AnalogValue"),
                Identifiers.AnalogItemType,
                new NodeFactory.InstantiationCallback() {
                    @Override
                    public boolean includeOptionalNode(NodeId typeDefinitionId, QualifiedName browseName) {
                        return true;
                    }
                }
            );

            node.setBrowseName(newQualifiedName("AnalogValue"));
            node.setDisplayName(LocalizedText.english("AnalogValue"));
            node.setDataType(Identifiers.Double);
            node.setValue(new DataValue(new Variant(3.14d)));

            node.setEURange(new Range(0.0, 100.0));

            getNodeManager().addNode(node);
            dataAccessFolder.addOrganizes(node);
        } catch (UaException e) {
            logger.error("Error creating AnalogItemType instance: {}", e.getMessage(), e);
        }
    }

 
    
    @Override
    public void onDataItemsCreated(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsCreated(dataItems);
    }

    @Override
    public void onDataItemsModified(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsModified(dataItems);
    }

    @Override
    public void onDataItemsDeleted(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsDeleted(dataItems);
    }

    @Override
    public void onMonitoringModeChanged(List<MonitoredItem> monitoredItems) {
        subscriptionModel.onMonitoringModeChanged(monitoredItems);
    }

}
