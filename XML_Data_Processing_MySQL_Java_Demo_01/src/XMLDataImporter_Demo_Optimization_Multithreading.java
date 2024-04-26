import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.*;

public class XMLDataImporter_Demo_Optimization_Multithreading {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/XMLData?useSSL=false&serverTimezone=UTC";
    private static final String USER = "root";
    private static final String PASS = "744214Sgg";
    private static final String DIRECTORY_PATH = "F:\\Zhuomian\\XML_Data_Processing_MySQL_Java_Demo_01\\CJFD";  // 文件夹路径
    private static final String TABLE_NAME = "articles_01";

    private static final int THREAD_COUNT = 8; // 根据你的系统资源调整

    private static ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    // 列出所有需要提取的标签
    private static final String[] TAGS = {
            "篇名", "作者", "机构", "中文关键词", "中文摘要", "英文篇名", "英文作者", "英文摘要", "英文关键词",
            "引文", "基金", "光盘号", "文献号", "中文刊名", "拼音刊名", "英文刊名", "年", "期", "CN", "ISSN",
            "文件名", "页", "分类号", "专题代码", "子栏目代码", "专题名称", "全文", "专辑代码", "更新日期",
            "制作日期", "专题子栏目代码", "第一责任人", "SCI收录刊", "EI收录刊", "核心期刊", "中英文篇名",
            "复合关键词", "中英文摘要", "出版日期", "机标关键词", "SYS_VSM", "来源数据库", "页数",
            "文件大小", "中英文作者", "中英文刊名", "DOI", "正文快照", "作者简介", "文章属性", "允许全文上网",
            "允许检索", "印刷页码", "原文格式", "下载频次", "被引频次", "文献标识码", "文献类型标识",
            "期刊标识码", "来源标识码", "影响因子", "主题词", "KMC分类号", "语义树条码", "专题整刊代码",
            "复合专题代码", "网络总库专辑代码", "语种", "年期", "TABLENAME", "基金代码", "期刊栏目层次",
            "出版单位", "作者代码", "机构代码", "表名", "是否含新概念", "所含新概念名称", "新概念代码",
            "概念出处", "是否高下载", "是否高被引", "是否高他引", "他引频次", "是否基金文献", "机构作者代码",
            "文献作者", "卷期号", "注册DOI", "纸刊DOI", "EISSN", "来源代码", "THNAME", "通讯作者",
            "创新点名称", "创新点内容", "网络发布时间", "URLID", "优先文件名", "优先出版", "FFD", "SMARTS",
            "CSSCI期刊", "CSCD期刊", "统计源期刊", "热度", "省代码", "出版物代码"
    };

    private static void createTableIfNotExists(Connection conn) throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (");
        sql.append("id INT AUTO_INCREMENT PRIMARY KEY, ");  // 添加自增主键

        for (String tag : TAGS) {
            String columnName = tag.replaceAll("\\s+", "_").replaceAll("[^a-zA-Z0-9_]", "");
            if (columnName.isEmpty()) {
                columnName = "unknown_column";  // 如果正则替换后列名为空，赋予默认列名
            }

            if ("SMARTS".equals(tag)) {
                sql.append("`").append(tag).append("` LONGTEXT NULL, ");
            }else if ("全文".equals(tag)){
                sql.append("`").append(tag).append("` LONGTEXT NULL, ");
            } else {
                sql.append("`").append(tag).append("` TEXT NULL, ");
            }
        }
        sql.delete(sql.length() - 2, sql.length()); // Remove the last comma
        sql.append(");");

        System.out.println("Executing SQL: " + sql);  // 打印 SQL 语句以供调试

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql.toString());
        }
    }



    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS)) {
            createTableIfNotExists(conn);
            List<File> files = listTextFiles(DIRECTORY_PATH);
            int totalFiles = files.size();
            CountDownLatch latch = new CountDownLatch(totalFiles);

            for (File file : files) {
                executor.submit(() -> {
                    try {
                        processFile(conn, file);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await(); // 等待所有文件处理完成
            executor.shutdown();
            System.out.println("All files processed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<File> listTextFiles(String directoryPath) {
        List<File> textFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(directoryPath), "*.txt")) {
            for (Path path : stream) {
                textFiles.add(path.toFile());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return textFiles;
    }

    private static void processFile(Connection conn, File file) throws Exception {
        Map<String, String> data = new HashMap<>();
        boolean recordStarted = false;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "GBK"))) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("<REC>")) {
                    if (recordStarted && !data.isEmpty()) {
                        insertData(conn, data);
                        data.clear();
                    }
                    recordStarted = true;
                } else if (recordStarted) {
                    for (String tag : TAGS) {
                        if (line.startsWith("<" + tag + ">=")) {
                            String content = line.substring(line.indexOf('=') + 1).trim();
                            content = removeTrailingSemicolon(content); // 去除尾部的分号
                            data.put(tag, content);
                            break;
                        }
                    }
                }
            }

            if (recordStarted && !data.isEmpty()) {
                insertData(conn, data);  // 插入最后一个记录
            }
        }
    }

    private static String removeTrailingSemicolon(String content) {
        if (content.endsWith(";")) {
            return content.substring(0, content.length() - 1);
        }
        return content;
    }

    private static void printProgress(int processed, int total) {
        int percent = (int) (((double) processed / total) * 100);
        StringBuilder bar = new StringBuilder("[");
        for (int i = 0; i < 100; i += 10) {
            if (i < percent) {
                bar.append("=");
            } else if (i == percent) {
                bar.append(">");
            } else {
                bar.append(" ");
            }
        }
        bar.append("] ").append(percent).append("%");
        System.out.print("\r" + bar.toString());
    }

    private static String extractTagContent(String text, String tag) {
        Pattern pattern = Pattern.compile(Pattern.quote(tag) + "(.*)");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    private static void insertData(Connection conn, Map<String, String> data) throws Exception {
        StringBuilder sql = new StringBuilder("INSERT INTO " + TABLE_NAME + " (");
        StringBuilder values = new StringBuilder("VALUES (");
        for (String tag : TAGS) {
            if (data.containsKey(tag)) {
                sql.append(tag).append(", ");
                values.append("?, ");
            }
        }
        sql.setLength(sql.length() - 2); // 移除多余的逗号和空格
        values.setLength(values.length() - 2);
        sql.append(") ").append(values).append(");");

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            int index = 1;
            for (String tag : TAGS) {
                if (data.containsKey(tag)) {
                    pstmt.setString(index++, data.get(tag));
                }
            }
            pstmt.executeUpdate();
        }
    }
}
