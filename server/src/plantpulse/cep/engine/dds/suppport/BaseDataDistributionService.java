package plantpulse.cep.engine.dds.suppport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import plantpulse.cep.engine.dds.DataDistributionService;
import plantpulse.cep.engine.dds.processor.DDSMessageProcessor;
import plantpulse.cep.engine.dds.processor.DDSTypes;
import plantpulse.cep.service.support.tag.TagCacheFactory;
import plantpulse.domain.Tag;

/**
 * BaseDataDistributionService
 * 
 * @author leesa
 *
 */
public class BaseDataDistributionService implements DataDistributionService {

	private static final Log log = LogFactory.getLog(DataDistributionService.class);

	private boolean IS_TAG_DATA_DISTRIBUATION = true;

	private boolean IS_ASSET_DATA_DISTRIBUTION = true;

	private DDSMessageProcessor processor = null;

	public BaseDataDistributionService(boolean IS_TAG_DATA_DISTRIBUATION, boolean IS_ASSET_DATA_DISTRIBUTION) {
		this.IS_TAG_DATA_DISTRIBUATION = IS_ASSET_DATA_DISTRIBUTION;
		this.IS_ASSET_DATA_DISTRIBUTION = IS_ASSET_DATA_DISTRIBUTION;
		this.processor = new DDSMessageProcessor();
	}

	@Override
	public void sendTagPoint(String tag_id, JSONObject data) {
		if (IS_TAG_DATA_DISTRIBUATION) {
			Tag tag = TagCacheFactory.getInstance().getTag(tag_id);
			if (tag != null) {
				processor.sendTagTypeData(DDSTypes.TAG_POINT, tag, data);
				
			} else {
				log.warn("Tag does not exist in the tag cache factory : tag_id=[" + tag_id + "]");
			}
		}
	}

	@Override
	public void sendTagAlarm(String tag_id, JSONObject data) {
		if (IS_TAG_DATA_DISTRIBUATION) {
			Tag tag = TagCacheFactory.getInstance().getTag(tag_id);
			if (tag != null) {
				processor.sendTagTypeData(DDSTypes.TAG_ALARM, tag, data);
			} else {
				log.warn("Tag does not exist in the tag cache factory : tag_id=[" + tag_id + "]");
			}
		}
	}

	@Override
	public void sendAssetData(String asset_id, JSONObject data) {
		if (IS_ASSET_DATA_DISTRIBUTION) {
			// processor.sendAssetTypeData(DDSTypes.ASSET_DATA, asset_id, data);
		}
	}

	@Override
	public void sendAssetAlarm(String asset_id, JSONObject data) {
		if (IS_ASSET_DATA_DISTRIBUTION) {
			processor.sendAssetTypeData(DDSTypes.ASSET_ALARM, asset_id, data);
		}
		;
	}

	@Override
	public void sendAssetEvent(String asset_id, JSONObject data) {
		if (IS_ASSET_DATA_DISTRIBUTION) {
			processor.sendAssetTypeData(DDSTypes.ASSET_EVENT, asset_id, data);
		}
		;
	}

	@Override
	public void sendAssetContext(String asset_id, JSONObject data) {
		if (IS_ASSET_DATA_DISTRIBUTION) {
			processor.sendAssetTypeData(DDSTypes.ASSET_CONTEXT, asset_id, data);
		}
		;
	}

	@Override
	public void sendAssetAggregation(String asset_id, JSONObject data) {
		if (IS_ASSET_DATA_DISTRIBUTION) {
			processor.sendAssetTypeData(DDSTypes.ASSET_AGGREGATION, asset_id, data);
		}
		;
	}

}
