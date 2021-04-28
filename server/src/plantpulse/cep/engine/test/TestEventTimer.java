package plantpulse.cep.engine.test;

import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import plantpulse.cep.engine.CEPEngineManager;
import plantpulse.event.TestEvent;

public class TestEventTimer {

	private Timer timer;

	public void start() {
		timer = new Timer("PP_TEST_EVENT_TIMER");
		timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {

				Random random = new Random();
				int f = random.nextInt(11);

				// 테스트 이벤트
				random = new Random();
				int a = random.nextInt(10);//
				int b = random.nextInt(300);//
				int c = random.nextInt(30);//
				float k = random.nextFloat();

				TestEvent test = new TestEvent();
				test.setTimestamp(new Date().getTime());
				test.setId("ID_" + System.currentTimeMillis());
				test.setValue(k);
				test.setValue_1(a);
				test.setValue_2(b);
				test.setValue_3(c);

				CEPEngineManager.getInstance().getProvider().getEPRuntime().sendEvent(test);

				// 태그 이벤트
				/*
				 * plantpulse.event.opc.Point tag = new
				 * plantpulse.event.opc.Point();
				 * tag.setTimestamp(System.currentTimeMillis());
				 * tag.setSite_id("SITE_00001"); tag.setOpc_id("SITE_00001");
				 * tag.setId("TAG_00000"); tag.setName("TEST.TAG_00000");
				 * tag.setGroup_name("TEST_GROUP"); tag.setError_code(192);
				 * tag.setType("float"); tag.setValue("0.123");
				 * tag.setError_code(192); tag.setQuality(0);
				 * 
				 * CEPEngineManager.getInstance().getProvider().getEPRuntime().
				 * sendEvent(tag);
				 * 
				 */

			}
		}, 1000, 1000);
	}

	public void stop() {
		timer.cancel();
	}

}
