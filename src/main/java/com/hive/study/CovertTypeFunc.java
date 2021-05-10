package com.hive.study;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * hive 自定义函数
 *
 * 1. 实现UDF，重载evaluate方法
 * 2. 打包成jar : mvn clean package -Dmaven.test.skip=true
 * 3. 将jar拷贝到hive classpath下:
 *  hive> add jar $project.dir/target/hive-plugin-1.0-SNAPSHOT.jar;
 *  hive> list jars; // 可以检查jar是否成功添加到hive classpath下
 * 4. 创建临时函数: hive> create temporary function convert_type as 'com.hive.study.CovertTypeFunc';
 * 5. 运行hql到目标表: select convert_type(<your field>) from target_table;
 *
 * author: xiaolian
 * date: 2021/5/10 下午11:00
 */
public class CovertTypeFunc extends UDF {

    private static final Map<String, String> typeMap = new HashMap<>();

    // TODO 可按需添加其他类型
    static {
        typeMap.put("1", "住宅");
        typeMap.put("2", "公寓");
        typeMap.put("3", "商铺");
        typeMap.put("4", "写字楼");
        typeMap.put("5", "车位");
        typeMap.put("6", "储藏室");
        typeMap.put("7", "其他");
    }

    public Text evaluate(final Text s){
        if(s==null){
            return null;
        }

        String line = s.toString();
        String[] splits = line.split(",");

        StringBuilder builder = new StringBuilder();
        for (String split : splits) {
            builder.append(typeMap.get(split)).append(",");
        }

        String result = builder.toString();

        return new Text(result.substring(0, result.length()-1));
    }

}
