package com.foo.udtf;

import groovy.json.JsonException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 25546
 * 自定义的udtf类 用于解析event中 的 一对多字段
 */
public class EventJsonUDTF extends GenericUDTF {
    //initialize 方法 用于指定输出参数的名称 和类型
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws
            UDFArgumentException {
        ArrayList<String> fieldNames=new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs=new ArrayList<>();
        //事件名称
        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //事件 json数据
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }
    //核心的处理方法 输入一条数据 输出多条数据
    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取传入的 et
        String input = objects[0].toString();
        // 如果传进来的数据为空，直接返回过滤掉该数据
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                // 获取一共有几个事件 点赞 评论 等
                JSONArray ja = new JSONArray(input);
                if (ja == null) {
                    return;
                }
                // 循环遍历每一个事件
                for (int i = 0; i < ja.length(); i++) {
                    String[] result = new String[2];
                    try {
                        // 取出每个的事件名称（ad/facoriters）
                        result[0] = ja.getJSONObject(i).getString("en");
                        // 取出每一个事件整体
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    // 将结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
    //当前没有处理的数据时候才会被调用 清理代码 或者 产生一些额外的输出
    @Override
    public void close() throws HiveException {

    }
}
