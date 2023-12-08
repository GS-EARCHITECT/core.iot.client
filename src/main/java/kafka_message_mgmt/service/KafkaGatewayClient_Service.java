package kafka_message_mgmt.service;

import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import kafka_message_mgmt.model.IOTAssetReading;
import kafka_message_mgmt.model.IOTSensorClient_Repo;

@Service("kafkaGatewayClientServ")
public class KafkaGatewayClient_Service 
{
	private static final Logger logger = LoggerFactory.getLogger(KafkaGatewayClient_Service.class);

	@Autowired
	private IOTSensorClient_Repo iotSensorClientRepo;

	@Autowired
	private KafkaIGatewayClientProcess_Service kafkaGatewayClientProcessServ;

	@Scheduled(fixedRate = 3000)
	public void createMessage() {
		CopyOnWriteArrayList<IOTAssetReading> iotAssetReadings = iotSensorClientRepo.getReadingsNotDone();

		if (iotAssetReadings != null && iotAssetReadings.size() > 0) {
			logger.info("running for size :" + iotAssetReadings.size());

			for (int i = 0; i < iotAssetReadings.size(); i++) {
				kafkaGatewayClientProcessServ.createGatewayMessage(iotAssetReadings.get(i));
				iotSensorClientRepo.setDoneFlag(iotAssetReadings.get(i).getReadingSeqNo());
			}
		}
		return;

	}

}