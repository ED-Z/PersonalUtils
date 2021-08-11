import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Pgsql2Mysql {

    static Map<String, String> commentMap = new HashMap<>();
    static Pattern commentPattern = Pattern.compile(".*`(.*?)`\\sIS\\s(.*?);");


    public static void main(String[] args) throws Exception {

        String[] list = new String[1];
        list[0] = ".";
        for (String s : getTxtFileList(list)) {
            File file = new File(s);
            try (Reader reader = new FileReader(file); Writer writer = new CharArrayWriter(); Writer out = new FileWriter("mysql_5",true)) {
                reader.transferTo(writer);
                String in = writer.toString();
                in = in.replaceAll(" COLLATE \".*?\".\".*?\"", "");
                in = in.replaceAll("\"", "`");
                in = in.replaceAll("DEFAULT nextval\\(\\S*?\\)","");
                String pk = getPK(in);
                String struct;
                if(pk!=null) {
                    struct = getMysqlStruct(getStructSql(in));
                    out.append(struct);
                    out.append(pk);

                }else {
                    struct = getMysqlStruct(getStructSql(in+"--"));
                    out.append(struct);
                }

                System.out.println("转换成功！");
            }
        }


    }

    static String getMysqlStruct(String pgSql) {
        StringBuilder sb = new StringBuilder();
        Pattern getCommentPattern = Pattern.compile("([\\s\\S]*?)(COMMENT[\\s\\S]*)");
        Matcher matcher = getCommentPattern.matcher(pgSql);
        if (matcher.matches()) {
            String createSql = matcher.group(1);
            String commentSql = matcher.group(2);
            for (String s : commentSql.lines().collect(Collectors.toList()))
                getCommentMap(s);
            for (String l : createSql.lines().collect(Collectors.toList())) {
                for (String k : commentMap.keySet()) {
                    if (l.contains("`"+k+"`") && l.contains(",")) {
                        l = l.replaceAll(",$", " COMMENT " + commentMap.get(k) + ",");
                        break;
                    } else if (l.contains("`"+k+"`")) {
                        l = l + " COMMENT " + commentMap.get(k);
                        break;
                    }
                }
                sb.append(l).append("\n");
            }
        } else {
            System.out.println("没有注释");
            return pgSql;
        }
        return sb.toString();

    }

    public static void getCommentMap(String origin) {
        if (origin.length() == 0) return;
        Matcher m = commentPattern.matcher(origin);
//        System.out.println(origin);
        if (m.matches()) {

            commentMap.put(m.group(1), m.group(2));
        } else {
            System.out.println("error in parse string");
        }
//        System.out.println(commentMap);
    }

    static String getPK(String str) {
        Pattern pattern;
        pattern = Pattern.compile(
                "Primary Key structure for table \\w*?[\\s\\S]*?" +
                        "-- ----------------------------([\\s\\S]*)");
        Matcher m = pattern.matcher(str);
        if (m.find()) {
            return m.group(1).replaceAll("\"", "`");
        } else {
            System.out.println("无主键!");
            return null;
        }

    }

    static String getStructSql(String str) throws Exception {
        Pattern pattern;
        pattern = Pattern.compile(
                "Table structure for \\w*?[\\s\\S]*?" +
                        "-- ----------------------------([\\s\\S]*?)--");
        Matcher m = pattern.matcher(str);
        if (m.find()) {
            return m.group(1);
        } else {
            System.out.println("解析失败：" + str);
            throw new Exception("无法获得表结构语句");
        }

    }

    public static List<String> getTxtFileList(String[] args) throws Exception {
        if (args.length != 1) {
            throw new Exception("error args");
        }
        String path = args[0];
        File dirPath = new File(path);
        List<String> filesString = new ArrayList<>();
        File[] tempList = dirPath.listFiles();
        assert tempList != null;
        if (tempList.length == 0) {
            throw new Exception("这是一个空目录");
        }
        for (File file : tempList) {
            if (file.isFile() && file.getName().endsWith(".sql")) {
                filesString.add(file.getName());
            }
        }
        return filesString;
    }

}
