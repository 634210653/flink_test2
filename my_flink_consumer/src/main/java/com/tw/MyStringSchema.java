package com.tw;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;

import java.io.ByteArrayInputStream;

public class MyStringSchema implements DeserializationSchema<MyString> {

    @Override
    public MyString deserialize(byte[] bytes) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);

        MyString ret = null;
        try{

            ret = AvroKryoSerializerUtils.getAvroUtils().createAvroSerializer(com.tw.MyString.class).deserialize( new DataInputViewStreamWrapper(stream));


        }catch (Exception e){
            e.printStackTrace();
        }

        return ret;
    }

    @Override
    public boolean isEndOfStream(MyString myString) {
        return false;
    }

    @Override
    public TypeInformation<MyString> getProducedType() {
        return TypeInformation.of(MyString.class);
    }
}
