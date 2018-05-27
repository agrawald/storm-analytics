package au.uni.melb.cloud.computing.analytics.bolt;

import au.uni.melb.cloud.computing.analytics.config.RabbitMqConfig;
import au.uni.melb.cloud.computing.analytics.utils.Constants;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class RabbitPublisherBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitPublisherBolt.class);
    private final static Gson gson = new Gson();
    private final String qName;
    private OutputCollector outputCollector;
    private Channel channel;

    public RabbitPublisherBolt(final String qName) {
        this.qName = qName;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        outputCollector = collector;
        try {
            channel = RabbitMqConfig.INSTANCE.getConnection().createChannel();
            if (!Constants.Q_TWEETS.getName().equals(qName)) {
                channel.queueDeclare(qName, true, false, false, Collections.singletonMap("x-message-ttl", 10000));
            } else {
                channel.queueDeclare(qName, true, false, false, Collections.singletonMap("x-max-length", 1000));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Tuple: {}", tuple.toString());
        String json = gson.toJson(tuple.getValue(0));
        try {
            channel.basicPublish("", qName, null, json.getBytes());
            outputCollector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();
            outputCollector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
