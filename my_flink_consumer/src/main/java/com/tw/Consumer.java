package com.tw;



import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.ByteArrayInputStream;
import java.util.Properties;

public class Consumer {


    public  void runJob(){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.207.20.217:9092");
        properties.setProperty("group.id", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableForceAvro();

//        AvroDeserializationSchema<MyString> schema = AvroDeserializationSchema.forSpecific(MyString.class);

        FlinkKafkaConsumer consumer  =     new FlinkKafkaConsumer<MyString>("wiki-results", new MyStringSchema(),properties);

        DataStream<MyString> inputStream = env.addSource(consumer);

        inputStream.map(MyString::getMessage).print();

        try {
            env.execute();
        }catch (Exception e){

        }
    }


}
