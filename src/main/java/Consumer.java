import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    private static final String server = "localhost:9092";
    private static final String topic = "default-topic";
    private static final String group = "default-group";

    private static final String username = "kafka";
    private static final String password = "kafka";

    private static KafkaConsumer<String, String> consumer;

    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) throws InterruptedException {
        // 종료 프로세스
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        // Consumer 프로세스
        init();
        consume();

        latch.await();

        // 종료
        consumer.close();
        log.info("Consumer closed.");
    }

    private static void init() {
        Properties props = new Properties();

        // 기본 properties
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 접근제어 properties
        // SECURITY_PROTOCOL_CONFIG : 보안 프로토콜을 설정합니다. 현재 서버에 적용된 값은 SASL_PLAINTEXT입니다.
        // SASL_MECHANISM : SASL 메커니즘을 설정합니다. 현재 서버에 적용된 값은 "PLAIN"입니다.
        // SASL_JAAS_CONFIG : SASL 구성 정보를 작성합니다.
        //   - org.apache.kafka.common.security.plain.PlainLoginModule required : 서버에 적용된 로그인 모듈인 PlainLoginModule로 설정합니다.
        //   - username, password : 클라이언트가 kafka 서버에 접근하기 위한 유저명과 비밀번호 정보를 설정합니다.
        //                          서버에 해당 유저명과 비밀번호에 맞는 값이 없거나, 토픽에 대해 해당 유저가 가진 권한이 없을 때는 접근 불가합니다.
        //   - 맨 마지막에 세미콜론(;)을 반드시 붙여 주어야 합니다.
        //props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        //props.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        //props.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
        //        "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + username + "' password='" + password + "';");

        consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("Subscribe topic : " + topic);
        } catch (Exception e) {
            log.warn("Consumer initialize error : " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void consume() {
        try {
            while (true) {  // ShutdownHook 실행 시 호출되는 consumer.wakeup()에 의해 WakeupException이 발생하며 루프가 종료됩니다.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 읽어온 메시지 처리 (key, value, partition, offset 정보를 로그로 출력합니다)
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() +
                            "\nValue : " + record.value() +
                            "\nPartition : " + record.partition() + ", Offset : " + record.offset());
                }

                consumer.commitAsync();
            }
        } catch (TopicAuthorizationException e) {
            log.warn("Not authorized to access topic : " + topic); // Topic 접근 권한이 없을 때 발생하는 Exception
        } catch (GroupAuthorizationException e) {
            log.warn("Not authorized to access group : " + group); // Consumer Group에 대해 권한이 없을 때 발생하는 Exception
        } catch (SaslAuthenticationException e) {
            log.warn("Authentication failed : Invalid username or password"); // 해당 유저, 비밀번호 정보가 서버에 없을 때 발생하는 Exception
        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.warn("Consume error : " + e.getMessage());
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
