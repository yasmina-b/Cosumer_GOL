import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

public class MyConsumer {
    public static String bootstrapServers="127.0.0.1:9092";
    public static String grp_id="third_app";
    public static String endTopic="endTopic";
    public static String lifecycleTopic="lifecycleTopic";
    public static int deadCellsCounter = 0;
    public static int dividedCellsCounter = 0;
    public static int fullCellsCounter = 0;
    public static Timer timer = new Timer();
    public static Logger logger= LoggerFactory.getLogger(MyConsumer.class.getName());
    public static boolean taskExecuted = false;

    public static void main(String[] args) {
        Properties properties = new Properties();
        //Logger logger= LoggerFactory.getLogger(MyConsumer.class.getName());

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, CellEvents> consumer = new KafkaConsumer<String, CellEvents>(properties, new StringDeserializer(),new KafkaJsonDeserializer<CellEvents>(CellEvents.class));// Subscribe to the topic
        consumer.subscribe(Arrays.asList(lifecycleTopic, endTopic));

        while (true) {
            ConsumerRecords<String, CellEvents> recordsLifecycle=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, CellEvents> record:recordsLifecycle){
                timer.cancel();
                timer.purge();
                timer = new Timer();
                taskExecuted = false;
                if(record.value()!= null) {
                    logger.info("Key: " + record.key() + ", Event type: " + record.value().eventType + ", Cell name: " + record.value().cellID + ", Time: " + LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), TimeZone.getDefault().toZoneId()));
                    switch(record.value().eventType) {
                        case CELL_DIED:
                            deadCellsCounter++;
                            break;
                        case CELL_ATE:
                            fullCellsCounter++;
                            break;
                        case CELL_DIVIDED:
                            dividedCellsCounter++;
                            break;
                    }
                }
            }
            if((deadCellsCounter > 0 || dividedCellsCounter > 0 || fullCellsCounter > 0 ) && !taskExecuted){
                taskExecuted = true;
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        logger.info("Number of cells which died " + deadCellsCounter);
                        logger.info("Number of cells which divided " + dividedCellsCounter);
                        logger.info("Number of cells which ate " + fullCellsCounter);
                    }
                }, 10000);
            }
        }
    }
}
