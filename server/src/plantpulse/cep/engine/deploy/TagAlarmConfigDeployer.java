package plantpulse.cep.engine.deploy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import plantpulse.cep.dao.TagDAO;
import plantpulse.cep.engine.async.AsynchronousExecutor;
import plantpulse.cep.engine.async.Worker;
import plantpulse.cep.engine.logging.EngineLogger;
import plantpulse.cep.engine.thread.AsyncInlineExecuterFactory;
import plantpulse.cep.service.AlarmConfigService;
import plantpulse.cep.service.support.alarm.UserDefinedTagAlarm;
import plantpulse.cep.service.support.alarm.UserDefinedTagAlarmDeployer;
import plantpulse.domain.AlarmConfig;
import plantpulse.domain.Tag;

public class TagAlarmConfigDeployer implements Deployer  {

	private static final Log log = LogFactory.getLog(TagAlarmConfigDeployer.class);

	public void deploy() {

		try {
			
			//
			final AtomicLong count = new AtomicLong(0);

			log.info("Tag band alarm deploy started...");
			
			// 알람 설정 (태그밴드 및 EQL 고급 알람 배치)
			AsynchronousExecutor exec = new AsynchronousExecutor();
			exec.execute(new Worker() {
				
				public String getName() {
					return "TagAlarmConfigDeployer";
				};

				public void execute() {

					//-------------------------------------------------------------------------------------------
					// 1. 밴드 알람 배포
					//-------------------------------------------------------------------------------------------
					try {
						long start = System.currentTimeMillis();
						log.info("Tag band alarm deploy started...");
						final AlarmConfigService service = new AlarmConfigService();
						List<AlarmConfig> list = service.getAlarmConfigListAll();
						if (list != null) {
							log.info("Tag band alarm size = [" + list.size() + "]");
						};
						ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
						for (int i = 0; list != null && i < list.size(); i++) {
							final AlarmConfig config = list.get(i);
							executor.execute(new BandAsyncTask(config, service, count));
						}
						executor.shutdown();
						long end = System.currentTimeMillis() - start;
						log.info("Tag band alarm deploy completed : process_time=[" + end + "]ms");
					} catch (Exception e) {
						EngineLogger.error("태그 밴드 알람 설정을 배치하는 도중 오류가 발생하였습니다 : " + e.getMessage());
						log.error(e);
					}
					
					
					//-------------------------------------------------------------------------------------------
					// 1. 사용자 정의 알람 배포 (태그에 정의된 사용자 정의 알람 사용할 경우, 알람 설정 배치)
					//-------------------------------------------------------------------------------------------
					try {
						long start = System.currentTimeMillis();
						log.info("Tag user-defined alarm deploy started...");
						TagDAO tag_dao = new TagDAO(); //
						List<Tag> tag_list = tag_dao.selectTagAll();
						ExecutorService executor = AsyncInlineExecuterFactory.getExecutor();
						for (int i = 0; tag_list != null && i < tag_list.size(); i++) {
							Tag tag = tag_list.get(i);
							executor.execute(new UDAsyncTask(tag));
						}
						executor.shutdown();
						long end = System.currentTimeMillis() - start;
						log.info("Tag user-define alarm deploy completed : process_time=[" + end + "]ms");
					} catch (Exception e) {
						EngineLogger.error("태그 사용자-정의 알람 설정을 배치하는 도중 오류가 발생하였습니다 : " + e.getMessage());
						log.error(e);
					}
				};
			});

			

		} catch (Exception ex) {
			EngineLogger.error("알람 설정을 배치하는 도중 오류가 발생하였습니다 : 에러=" + ex.getMessage());
			log.warn("Alarm configuration deploy error : " + ex.getMessage(), ex);
		}
	}
	
	
	/**
	 * BandAsyncTask
	 * @author leesa
	 *
	 */
	class BandAsyncTask implements Runnable {

		private AlarmConfig config;
		private AlarmConfigService service;
		private AtomicLong cnt;

		public BandAsyncTask(AlarmConfig config, AlarmConfigService service, AtomicLong cnt) {
			this.config = config;
			this.service = service;
			this.cnt = cnt;
		}

		@Override
		public void run() {
			//
			try {
				String tag_id = config.getAlarm_config_id().replaceAll("ALARM_CONFIG_", "");
				TagDAO tag_dao = new TagDAO(); //
				Tag tag = tag_dao.selectTag(tag_id);
				if (tag == null) { // 원본 태그가 존재하지 않으면, 삭제된 태그이므로 설정도 같이 삭제하고 경고한다.
					log.info("Already a deleted tag, it also removes the alarm setting : tag_id=[" + tag_id + "], alarm_config_id=[" + config.getAlarm_config_id() + "]");
					service.deleteAlarmConfig(config.getAlarm_config_id());
				} else {
					if ("TAG".equals(StringUtils.defaultString(config.getAlarm_type(), ""))) {
						service.deployAlarmConfigForTagBand(config, tag); // BAND
						log.debug("Alarm deployed : id=[" + config.getAlarm_config_id() + "]");
						cnt.incrementAndGet();
					} else {
						service.deployAlarmConfig(config); // EQL
						log.debug("Alarm deployed : id=[" + config.getAlarm_config_id() + "]");
						cnt.incrementAndGet();
					}
					;
				}
			} catch (Exception ex) {
				EngineLogger.error("태그 알람 설정을 배치하는 도중 오류가 발생하였습니다 : 알람설정ID=[" + config.getAlarm_config_id() + "], 에러=["
						+ ex.getMessage() + "]");
			}
		}

	}
	
	
	/**
	 * UDAsyncTask
	 * @author leesa
	 *
	 */
	class UDAsyncTask implements Runnable {

		private Tag tag;

		public UDAsyncTask(Tag tag) {
			this.tag = tag;
		}

		@Override
		public void run() {
			try {
				if (StringUtils.isNotEmpty(tag.getUser_defined_alarm_class())) {
					UserDefinedTagAlarm user_defined_alarm = (UserDefinedTagAlarm) Class.forName(tag.getUser_defined_alarm_class()).newInstance();
					UserDefinedTagAlarmDeployer deployer = new UserDefinedTagAlarmDeployer();
					//
					deployer.deploy(tag, user_defined_alarm.configure(tag));
				}
				;
			} catch (Exception ex) {
				EngineLogger.error("태그 사용자-정의 알람 설정을 배치하는 도중 오류가 발생하였습니다 : 알람설정ID=[" + tag.getTag_id() + "], 에러=["
						+ ex.getMessage() + "]");
			}
		}

	}

}


