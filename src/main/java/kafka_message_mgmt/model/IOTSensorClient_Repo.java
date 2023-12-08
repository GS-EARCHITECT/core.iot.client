package kafka_message_mgmt.model;

import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Transactional(propagation=Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
@Repository("iotSensorClientRepo")
public interface IOTSensorClient_Repo extends JpaRepository<IOTAssetReading, Long> 
{
	@Query(value = "select * from IOT_ASSET_READINGS_DATA where upper(trim(doneflag)) <> 'Y' order by EX_READING_SEQ_NO", nativeQuery = true)
	CopyOnWriteArrayList<IOTAssetReading> getReadingsNotDone();
	
	@Modifying
	@Query(value = "update IOT_ASSET_READINGS_DATA set doneflag = 'Y' where ex_reading_seq_no = :reading_seq_no", nativeQuery = true)
	void setDoneFlag(@Param("reading_seq_no") Long reading_seq_no);

		
}