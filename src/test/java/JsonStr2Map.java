import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonStr2Map {
    public static void main(String[] args) {
        jsonStr2Map("{\"data\":{\"id\":\"201068\",\"name\":\"testato8\",\"real_name\":\"就是帅8\",\"birth\":\"1997-03-15\",\"last_login_at\":\"2019-02-05 10:58:15\",\"grade_change_time\":\"2019-02-02 00:00:00\"}}");
//        String str = "{\"name\":\"li\",\"age\":23}";
//        String str = "{\"country\":\"US\",\"amount\":73.65569761420467,\"qty\":5,\"currency\":\"USD\",\"order_time\":1552026151162,\"category\":\"CLOTH\",\"device\":\"Andriod\",\"gender\":\"Male\",\"id\":\"a59c94d5-e5b6-0d71-dc46-cf5fc880cdfb\",\"first_name\":\"unknown\",\"age\":26}";
//        Matcher mat = Pattern.compile("((?<=:)(\\S+)(?=,)|(?<=:)(\\S+)(?=}))").matcher(str);
//        while (mat.find()) {
//            System.out.println(mat.group());
//        }
    }

    /**
     * 将Json字符串转为Map对象
     * @param jsonStr
     * @return
     */
    public static Map<String, String> jsonStr2Map(String jsonStr) {
//        Map<String, String> resultMap = new HashMap<>();
//        Pattern pattern = Pattern.compile("(\"\\w+\"):(\"[^\"]+\")");
//        Pattern pattern2 = Pattern.compile("(\"\\w+\"):([^\"]+)");
//        Matcher m = pattern.matcher(jsonStr);
//        Matcher m2 = pattern2.matcher(jsonStr);
//        String[] strs = null;
//        String[] strs2 = null;
//        while (m.find()) {
//            strs = m.group().split(":");
//            if(strs != null && strs.length == 2) {
//                resultMap.put(strs[0].replaceAll("\"", "").trim(), strs[1].trim().replaceAll("\"", ""));
//            }
//        }
//
//        while (m2.find()) {
//            strs2 = m2.group().split(":");
//            if(strs2 != null && strs2.length == 2) {
//                resultMap.put(strs2[0].replaceAll("\"", "").trim(), strs2[1].trim().replaceAll("\"", ""));
//            }
//        }
//        System.out.println(resultMap);
//        return resultMap;
//


        Map<String, String> resultMap = new HashMap<>();
        Pattern pattern = Pattern.compile("(\\w+\"):([^,]+)");
        Matcher m = pattern.matcher(jsonStr);
        String[] strs = null;

        while (m.find()) {
            strs = m.group().split(":");
            if(strs != null && strs.length == 2) {
                resultMap.put(strs[0].replaceAll("\"", "").trim(), strs[1].trim().replaceAll("\"", ""));
            }
        }
        System.out.println(resultMap);
        return resultMap;


    }




}