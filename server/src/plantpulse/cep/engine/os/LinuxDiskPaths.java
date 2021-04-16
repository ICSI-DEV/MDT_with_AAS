package plantpulse.cep.engine.os;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * LinuxDiskPaths
 * 
 * @author leesa
 *
 */
public class LinuxDiskPaths {
	
	public static final String[] paths = {
			"/home", 
			"/data", 
			"/data1",
			"/data2",
			"/data3",
			"/data4",
			"/data5",
			"/data6",
			"/data7",
			"/data8",
			"/data9",
			"/data10",
			"/"};
	
	public static List<Path> getPaths(){
		List<Path> list = new ArrayList<>();
		for(int i=0; i < paths.length; i++) {
			Path root  = FileSystems.getDefault().getPath(paths[i]);
			list.add(root);
			
		};
		return list;
	}

}
