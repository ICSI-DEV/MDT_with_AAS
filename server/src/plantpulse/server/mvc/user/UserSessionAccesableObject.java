package plantpulse.server.mvc.user;

import java.util.List;

import plantpulse.domain.Asset;
import plantpulse.domain.Site;
import plantpulse.domain.Tag;

/**
 * UserAccesableObject
 * 
 * @author leesa
 *
 */
public class UserSessionAccesableObject {

	private List<Site>  site_list;
	private List<Asset> area_list;
	private List<Asset> asset_list;
	private List<Tag>   tag_list;
	
	public List<Site> getSite_list() {
		return site_list;
	}
	public void setSite_list(List<Site> site_list) {
		this.site_list = site_list;
	}
	public List<Asset> getArea_list() {
		return area_list;
	}
	public void setArea_list(List<Asset> area_list) {
		this.area_list = area_list;
	}
	public List<Asset> getAsset_list() {
		return asset_list;
	}
	public void setAsset_list(List<Asset> asset_list) {
		this.asset_list = asset_list;
	}
	public List<Tag> getTag_list() {
		return tag_list;
	}
	public void setTag_list(List<Tag> tag_list) {
		this.tag_list = tag_list;
	}
	
	
}
