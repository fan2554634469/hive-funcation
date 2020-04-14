package com.foo.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
//解析基础的字段表 一对一表
public class BaseFieldUDF extends UDF {
    public String evaluate (String line,String jsonkeysString) throws JSONException {
        StringBuilder sb=new StringBuilder();
        //按照逗号切割jsonkey
        String[] jsonkeys=jsonkeysString.split(",");
        //处理 line
        String[] logContents=line.split("\\|");
        //进行合法校验
        if (logContents.length !=2 || StringUtils.isBlank(logContents[1])){
            return "";
        }
        //开始处理 json
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);
            //获取cm里面的对象
            JSONObject base=jsonObject.getJSONObject("cm");
            //循环遍历取值
            for (int i=0;i<jsonkeys.length;i++){
                String fileName=jsonkeys[i].trim();
                if (base.has(fileName)){
                    sb.append(base.getString(fileName)).append("\t");
                }else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        }catch (JSONException e){
            e.printStackTrace();
        }

        return sb.toString();
    }

    public static void main(String[] args) throws JSONException {
        String line="1585152070403|{\"cm\":{\"ln\":\"-100.0\",\"sv\":\"V2.4.2\",\"os\":\"8.1.7\",\"g\":\"7VC5JLYF@gmail.com\",\"mid\":\"1\",\"nw\":\"WIFI\",\"l\":\"en\",\"vc\":\"5\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"1\",\"t\":\"1585064987351\",\"la\":\"-32.5\",\"md\":\"sumsung-5\",\"vn\":\"1.2.0\",\"ba\":\"Sumsung\",\"sr\":\"I\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1585090891706\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"category\":\"65\"}},{\"ett\":\"1585143353585\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"1\",\"action\":\"4\",\"detail\":\"\",\"source\":\"3\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"0\"}},{\"ett\":\"1585053975598\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1585144935358\",\"action\":\"1\",\"type\":\"1\",\"content\":\"\"}},{\"ett\":\"1585056480281\",\"en\":\"active_foreground\",\"kv\":{\"access\":\"1\",\"push_id\":\"3\"}},{\"ett\":\"1585066395379\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1585147406082\",\"en\":\"praise\",\"kv\":{\"target_id\":5,\"id\":1,\"type\":3,\"add_time\":\"1585118657789\",\"userid\":7}}]}";
        String x = new BaseFieldUDF().evaluate(line,
                "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}
