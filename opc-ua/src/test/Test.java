package test;

import plantpulse.plugin.opcua.Main;

public class Test {

	public static void main(String[] args){
		Main m = new Main();
		m.main(new String[] {
			"C:\\Users\\leesa\\git\\plantpulse-plugin-opcua-server\\config\\opcua.server.config.json", "db"
		});

	}

}
