package com.lifoetech.storm.masteringApacheStorm.stormKafkaIntegration;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.daemon.nimbus;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.kafka.*;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

import static org.apache.storm.Config.TOPOLOGY_NAME;

public class KafkaTopology {
    public static void main(String[] args) {
        try {
            // zookeeper hosts for the Kafka cluster
            BrokerHosts zkHosts = new ZkHosts("zookeeper1:2181/kafka");

            // Create the KafkaSpout configuartion
            // Second argument is the topic name
            // Third argument is the zookeepr root for Kafka
            // Fourth argument is consumer group id
            SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "masteringApacheStorm-stormKafkaIntegration", "",
                    "id1");

            // Specify that the kafka messages are String
            // We want to consume all the first messages in the topic everytime
            // we run the topology to help in debugging. In production, this
            // property should be false
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            kafkaConfig.startOffsetTime = kafka.api.OffsetRequest
                    .EarliestTime();


            // Now we create the topology
            TopologyBuilder builder = new TopologyBuilder();

            // set the kafka spout class
            builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 2);

            // set the word and sentence bolt class
            builder.setBolt("WordBolt", new WordBolt(), 1).globalGrouping(
                    "KafkaSpout");
            builder.setBolt("SentenceBolt", new SentenceBolt(), 1)
                    .globalGrouping("WordBolt");


            Config config = new Config();

            config.setDebug(true);

            if(args.length == 0){
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
                Thread.sleep(10000);
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            } else{
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            }
        } catch (Exception ae) {
            ae.printStackTrace();
        }


    }
}
