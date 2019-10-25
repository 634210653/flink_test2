package com.tw;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

import java.io.ByteArrayOutputStream;


public class Producer {



    public void runJob() {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<WikipediaEditEvent> inputStream = env.addSource(new WikipediaEditsSource());

        env.getConfig().enableForceAvro();

        KeyedStream<WikipediaEditEvent, String> keyedStream = inputStream
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    @Override
                    public String getKey(WikipediaEditEvent event) {
                        return event.getUser();
                    }
                });


        DataStream<Tuple2<String, Long>> result = keyedStream
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("", 0L);
                    }

                    @Override
                    public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
                        accumulator.f0 = value.getUser();
                        accumulator.f1 += value.getByteDiff();
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return new Tuple2<>(a.f0, a.f1 + b.f1);
                    }
                });




        SerializationSchema<MyString> schema = new SerializationSchema<MyString>() {
            @Override
            public byte[] serialize(MyString myString) {

                ByteArrayOutputStream stream = new ByteArrayOutputStream();

                TypeInformation<MyString> info = TypeInformation.of(MyString.class);
                byte [] ret = null;
                try{
                    AvroKryoSerializerUtils.getAvroUtils().createAvroSerializer(MyString.class).serialize(myString,new DataOutputViewStreamWrapper(stream));
                    ret = stream.toByteArray();

                }catch (Exception e){
                    e.printStackTrace();
                }

                return ret;
            }
        };
        result.map(new MapFunction<Tuple2<String, Long>, MyString>() {
            @Override
            public MyString map(Tuple2<String, Long> tuple) {

                return  new MyString(tuple.toString());
            }
        })
                .addSink(new FlinkKafkaProducer<MyString>("10.207.20.217:9092", "wiki-results",new KeyedSerializationSchemaWrapper<MyString>(schema)));

        try {
            env.execute();
        }catch (Exception e){

        }
    }

}
