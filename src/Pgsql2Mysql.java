import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Pgsql2Mysql {

    static Map<String, String> commentMap = new HashMap<>();
    static Pattern commentPattern = Pattern.compile( ".*`(.*?)`\\sIS\\s(.*?);");
    static Pattern getCommentPattern = Pattern.compile("([\\s\\S]*?)(COMMENT[\\s\\S]*)");


    public static void main(String[] args) throws IOException {
        File file = new File("src/out.txt");
        try (Reader reader = new FileReader(file);Writer writer = new CharArrayWriter();Writer out = new FileWriter("src/sql.txt")){
            reader.transferTo(writer);
            String in = writer.toString();
            in = in.replaceAll(" COLLATE \".*?\".\".*?\"", "");
            in = in.replaceAll("\"","`");
            String struct = getMysqlStruct(getStructSql(in));
            String pk = getPK(in);
            out.write(struct);
            assert pk != null;
            out.write(pk);
            System.out.println("转换成功！");
        }


    }

    static String getMysqlStruct(String pgSql){
        StringBuilder sb = new StringBuilder();
        Matcher matcher = getCommentPattern.matcher(pgSql);
        if(matcher.matches()) {
            String createSql = matcher.group(1);
            String commentSql = matcher.group(2);
            for(String s:commentSql.lines().collect(Collectors.toList()))
                getCommentMap(s);
            for(String l:createSql.lines().collect(Collectors.toList())){
                for(String k: commentMap.keySet()){
                    if(l.contains(k)&&l.contains(",")) {
                        l = l.replaceAll(",", " COMMENT " + commentMap.get(k) + ",");
                        break;
                    }
                    else if(l.contains(k)) {
                        l = l + " COMMENT " + commentMap.get(k);
                        break;
                    }
                }
                sb.append(l).append("\n");
            }
        }
        else {
            System.out.println("error");
        }
        return sb.toString();

    }

    public static void getCommentMap(String origin){
        if(origin.length()==0) return;
        Matcher m = commentPattern.matcher(origin);
//        System.out.println(origin);
        if(m.matches()) {

            commentMap.put(m.group(1),m.group(2));
        }
        else {
            System.out.println("error in parse string");
            System.out.println(Arrays.toString(origin.getBytes()));
        }
    }

    static String getPK(String str){
        Pattern pattern;
        pattern = Pattern.compile(
                "Primary Key structure for table \\w*?\\r\\n" +
                        "-- ----------------------------([\\s\\S]*)");
        Matcher m = pattern.matcher(str);
        if(m.find()){
            return m.group(1).replaceAll("\"","`");
        }else {
            System.out.println("解析失败："+str);
            return null;
        }

    }

    static String getStructSql(String str){
        Pattern pattern;
        pattern = Pattern.compile(
                "Table structure for \\w*?\\r\\n" +
                        "-- ----------------------------([\\s\\S]*?)-- ----------------------------");
        Matcher m = pattern.matcher(str);
        if(m.find()){
            return m.group(1);
        }else {
            System.out.println("解析失败："+str);
            return null;
        }

    }

}
