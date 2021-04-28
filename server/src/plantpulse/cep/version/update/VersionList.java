package plantpulse.cep.version.update;

import java.util.ArrayList;
import java.util.List;

import plantpulse.cep.version.VersionUpdate;

public class VersionList {
	
	public static List<VersionUpdate> getList(){
		List<VersionUpdate> up_list = new ArrayList<>();
		up_list.add(new VersionUpdate_V6_0_0_20180909());
		up_list.add(new VersionUpdate_V6_0_1_20181006());
		up_list.add(new VersionUpdate_V6_0_1_20181012());
		up_list.add(new VersionUpdate_V6_5_0_20190330());
		up_list.add(new VersionUpdate_V7_0_0_20190826());
		up_list.add(new VersionUpdate_V7_0_0_20191013());
		up_list.add(new VersionUpdate_V7_1_0_20200405());
		up_list.add(new VersionUpdate_V7_1_0_20201207());
		up_list.add(new VersionUpdate_V7_1_0_20201208());
		up_list.add(new VersionUpdate_V8_1_0_20210207());
		return up_list;
	}

}
