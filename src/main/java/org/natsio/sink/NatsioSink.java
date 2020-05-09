package org.natsio.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import io.nats.client.Connection;
import io.nats.client.Nats;

public class NatsioSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(NatsioSink.class);
    private String topic;
    private String natsserver;
    private Connection nc;


    @Override
    public synchronized void start() {
        super.start();
        try {

            nc = Nats.connect(natsserver);

        } catch (Exception e) {

            try {
                nc.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }
    }


    @Override
    public synchronized void stop() {

        try {
            super.stop();
            System.exit(-1);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Status.READY;

            }

            String rawevt = new String(event.getBody());
            nc.publish(topic, rawevt.getBytes(StandardCharsets.UTF_8));
            nc.flush(Duration.ZERO);
            tx.commit();
            return Status.READY;

        } catch (Exception e) {
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch (Exception e2) {
                log.error("Rollback Exception:{}", e2);
            }
            log.error("NatsioSink Exception:{}", e);
            return Status.BACKOFF;
        } finally {
            tx.close();
        }

    }


        public void configure(Context context) {
            topic = context.getString("topic");
            if (topic == null) {
                throw new ConfigurationException("natsio topic must be specified.");
            }
            natsserver = context.getString("natsserver");
            if (natsserver == null) {
                throw new ConfigurationException("natsio server must be specified.");
            }
        }


}
