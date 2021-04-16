package plantpulse.cep.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIVariant;
import org.json.JSONArray;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.AccessBase;
import org.openscada.opc.lib.da.DataCallback;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.ItemState;
import org.openscada.opc.lib.da.Server;
import org.openscada.opc.lib.da.SyncAccess;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import plantpulse.cep.dao.ControlDAO;
import plantpulse.cep.dao.OPCDAO;
import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.service.client.StorageClient;
import plantpulse.domain.OPC;
import plantpulse.domain.Tag;

/**
 * ControlService
 * 
 * @author leesa
 *
 */
@Service
public class ControlService {

	private static final Log log = LogFactory.getLog(ControlService.class);

	private OPCDAO opc_dao;
	private TagDAO tag_dao;
	private ControlDAO control_dao;
	

	@Autowired
	private StorageClient storage_client;


	public ControlService() {
		this.opc_dao = new OPCDAO();
		this.tag_dao = new TagDAO();
		this.control_dao = new ControlDAO(); 
	};
	

	public JSONArray searchControlList(Map<String, String> params) throws Exception {
		try {
			List<Map<String, Object>> list =  control_dao.searchControlList(params);
			return JSONArray.fromObject(list);
		} catch (Exception e) {
			log.error("Select tag data list error : " + e.getMessage(), e);
			throw e;
		}
	}

	public void setValue(String tag_id, String set_value) throws Exception {
		//
		Tag tag = tag_dao.selectTag(tag_id);
		if(tag == null){
			throw new Exception("Tag[" + tag_id + "] does not exist.");
		}
		OPC opc = opc_dao.selectOpcInfoByOpcId(tag.getOpc_id());
		if(opc == null){
			throw new Exception("OPC[" + tag.getOpc_id() + "] does not exist.");
		}
		if(!opc.getOpc_type().equals("OPC")){
			throw new Exception("Tag datasource is not OPC Type.");
		}
		
		//
		setOpcValue(opc, tag, set_value); //
	}
	

	/**
	 * setOpcValue
	 * 
	 * @param opc
	 * @param tag
	 * @param set_value
	 * @throws Exception
	 */
	private void setOpcValue(OPC opc, Tag tag, String set_value) throws Exception {

		Server server = null;
		AccessBase access = null;

		try {

			// create connection information
			final ConnectionInformation ci = new ConnectionInformation();

			ci.setHost(opc.getOpc_server_ip());
			ci.setDomain(opc.getOpc_domain_name());
			ci.setUser(opc.getOpc_login_id());
			ci.setPassword(opc.getOpc_password());
			ci.setProgId(opc.getOpc_program_id());
			ci.setClsid(opc.getOpc_cls_id());
			//
			final String itemId = tag.getTag_name();

			// create a new server
			server = new Server(ci, Executors.newSingleThreadScheduledExecutor());

			// connect to server
			server.connect();
			// add sync access, poll every 500 ms
			access = new SyncAccess(server, 500);
			access.addItem(itemId, new DataCallback() {
				public void changed(Item item, ItemState state) {
					//
					try {
						if (state.getValue().getType() == JIVariant.VT_UI4) {
							log.info("VT_UI4 <<< " + state + " / value = " + state.getValue().getObjectAsUnsigned().getValue());
						} else {
							log.info("<<< " + state + " / value = " + state.getValue().getObject());
						}
					} catch (JIException e) {
						log.error("OPC Item chnaged value caputer error : " + e.getMessage(), e);
					}
				}
			});

			// Add a new group
			final Group group = server.addGroup("PP_CONTROL");
			
			// Add a new item to the group
			final Item item = group.addItem(itemId);

			// start reading
			access.bind();

			final JIVariant value = new JIVariant(set_value);
			int result = item.write(value);
			
			log.info("OPC Write result code : " +  result);
			
			//
			Thread.sleep(300);

		} catch (final JIException e) {
			log.error(String.format("%08X: %s", e.getErrorCode(), server.getErrorMessage(e.getErrorCode())), e);
			throw new Exception(String.format("%08X: %s", e.getErrorCode(), server.getErrorMessage(e.getErrorCode())), e);

		} finally {
			// stop reading
			if (access != null)
				access.unbind();
			if (server != null)
				server.disconnect();
		}
	}
}
