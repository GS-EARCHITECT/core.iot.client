package kafka_message_mgmt.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import kafka_message_mgmt.model.GatewayAssetReading;
import kafka_message_mgmt.model.IOTAssetReading;

@Service("kafkaGatewayClientProcessServ")
public class KafkaIGatewayClientProcess_Service 
{
	private static final Logger logger = LoggerFactory.getLogger(KafkaIGatewayClientProcess_Service.class);

	@Autowired
	private KafkaTemplate<String, GatewayAssetReading> kafkaTemplateRequest;

	@Value("${topic.name.iotreadingproducer}")
	private String topicmyProducer;

	@Async
	public synchronized void createGatewayMessage(IOTAssetReading iotAssetReading) {
		GatewayAssetReading gatewayAssetReading = new GatewayAssetReading();
		gatewayAssetReading.setAssetSeqNo(iotAssetReading.getAssetSeqNo());
		gatewayAssetReading.setOnDttm(iotAssetReading.getOnDttm());
		gatewayAssetReading.setReading(iotAssetReading.getReading());
		gatewayAssetReading.setSensorAssetSeqNo(iotAssetReading.getSensorAssetSeqNo());
		gatewayAssetReading.setResMeasureSeqNo(iotAssetReading.getResMeasureSeqNo());
		ListenableFuture<SendResult<String, GatewayAssetReading>> future = kafkaTemplateRequest.send(topicmyProducer,
				gatewayAssetReading);

		future.addCallback(new ListenableFutureCallback<SendResult<String, GatewayAssetReading>>() {

			@Override
			public void onSuccess(final SendResult<String, GatewayAssetReading> message) {
				logger.info("Ad No :" + message.getProducerRecord().value().getAssetSeqNo());
				// logger.info("updated schedule for ruleline :" +
				// message.getProducerRecord().value().getReferenceSeqNo());
			}

			@Override
			public void onFailure(final Throwable throwable) {
				logger.error("unable to send message= ", throwable);
			}
		});
		return;
	}

}