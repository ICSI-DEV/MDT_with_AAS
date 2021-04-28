package plantpulse.cep.engine.messaging.api;

public class MessagingAPI {

	public static void main(String[] args) {
		String tag_name = "TEMP01.A1.B1";
		tag_name=tag_name.replaceAll("\\.", " / ");
		System.out.println(tag_name);
		
		System.out.println(Integer.parseInt("20000"));
	}
}