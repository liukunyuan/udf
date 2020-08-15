package com.lky;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.json.JSONArray;
import org.json.JSONException;
import java.util.ArrayList;

@Description(name = "json_array",
        value = "_FUNC_(array_string) - Convert a string of a JSON-encoded array to a Hive array of strings.")
public class JsonArray extends  UDF{

    //add jar /home/hdp-credit/workdir/liukunyuan/project/tmp/udf.jar;
    // create temporary function json_array as 'com.lky.JsonArray';
    public ArrayList<String> evaluate(String jsonString) throws JSONException {

        if (StringUtils.isBlank(jsonString)) {
            return null;
        }

            JSONArray extractObject = new JSONArray(jsonString);
            ArrayList<String> result = new ArrayList<String>();
            for (int i = 0; i < extractObject.length(); i++) {
                result.add(extractObject.get(i).toString());
            }
            return result;

    }
}
