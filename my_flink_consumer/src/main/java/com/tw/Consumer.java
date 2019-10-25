package com.tw;



import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Consumer {


    public  void runJob(){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.207.20.217:9092");
        properties.setProperty("group.id", "test");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableForceAvro();

        AvroDeserializationSchema<MyString> schema = AvroDeserializationSchema.forSpecific(MyString.class);

//        DeserializationSchema<MyString> schema = new DeserializationSchema<MyString>() {
//            @Override
//            public MyString deserialize(byte[] bytes) throws IOException {
//
//               return MyString.fromByteBuffer(ByteBuffer.wrap(bytes));
//
//            }
//
//            @Override
//            public boolean isEndOfStream(MyString myString) {
//                return myString.;
//            }
//
//            @Override
//            public TypeInformation<MyString> getProducedType() {
//                return null;
//            }
//        };
        DataStream<MyString> inputStream = env.addSource(new FlinkKafkaConsumer<MyString>("wiki-results",  schema,properties));
//        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("wiki-results", new SimpleStringSchema(),properties));

        inputStream.map(MyString::getMessage).print();

        try {
            env.execute();
        }catch (Exception e){

        }
    }

}
