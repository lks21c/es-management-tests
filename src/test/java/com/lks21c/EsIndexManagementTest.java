package com.lks21c;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EsIndexManagementTest {
    private static final Logger logger = LogManager.getLogger(EsIndexManagementTest.class);

    @Autowired
    private Client client;

    @Autowired
    private RestClient restClient;

    @Test
    public void listIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        for (String line : lines) {
            String status = line.split(" ")[0];
            logger.info(line);
        }
    }

    @Test
    public void listRedIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cntRealtime = 0;
        int cntNotRealtime = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if ("red".equals(status)) {
                String indexName = line.split("red   ")[1].split(" ")[1];

                if (indexName.contains("realtime")) {
                    logger.info(cntRealtime++ + " " + indexName);
                } else {
                    logger.info(cntNotRealtime++ + " " + indexName);
                }
            }
        }

        logger.info("cntRealtime = " + cntRealtime);
        logger.info("cntNotRealtime = " + cntNotRealtime);
    }

    @Test
    public void deleteRealtimeRedIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if ("red".equals(status)) {
                String indexName = null;
                try {
                    indexName = line.split("red   ")[1].split(" ")[1];
                } catch (ArrayIndexOutOfBoundsException e) {
                    logger.info(indexName + " ");
                    e.printStackTrace();
                }
                if (indexName.contains("realtime")) {
                    client.admin().indices().prepareDelete(indexName).get();
                    logger.info(cnt++ + " delete realtime : " + indexName);
                } else {
//                    logger.info("not realtime : " + indexName);
                }
            }
        }
    }

    @Test
    public void deleteRealtimeOldIndices() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if (!"red".equals(status)) {
                LocalDate date = LocalDate.now();
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
                String dateString = date.toString(fmt);
                if (!line.contains(dateString)) {
                    String indexName = null;
                    if (line.indexOf("mel_") > 0) {
                        indexName = line.substring(line.indexOf("mel_"), line.length()).split(" ")[0];
                    } else if (line.indexOf("mtk_") > 0) {
                        indexName = line.substring(line.indexOf("mtk_"), line.length()).split(" ")[0];
                    }

                    if (indexName != null) {
                        if (indexName.contains("realtime")) {
                            try {
                                DateTimeFormatter fmt2 = null;
                                if (indexName.contains("st_profile") || indexName.contains("pv_profile") || indexName.contains("dl_profile")) {
                                    fmt2 = DateTimeFormat.forPattern("yyyyMMddHH");
                                } else {
                                    fmt2 = DateTimeFormat.forPattern("yyyyMMdd");
                                }

                                String dateFromIndex = indexName.split("_")[indexName.split("_").length - 1];
                                logger.info(dateFromIndex);

                                DateTime dateTimeFromIndex = fmt2.parseDateTime(dateFromIndex);

                                DateTime curDateTime = new DateTime();
                                int agingDays = Days.daysBetween(dateTimeFromIndex.toLocalDate(), curDateTime.toLocalDate()).getDays();

                                if (indexName.contains("profile_realtime")) {
                                    if (agingDays > 7) {
                                        client.admin().indices().prepareDelete(indexName).get();
                                        logger.info(cnt++ + " " + indexName + " deleted, agingDays = " + agingDays);
                                    }
                                } else {
                                    if (agingDays > 2) {
                                        client.admin().indices().prepareDelete(indexName).get();
                                        logger.info(cnt++ + " " + indexName + " deleted, agingDays = " + agingDays);
                                    }
                                }
                            } catch (Exception e) {
                                logger.info("exception with " + indexName + e.getMessage());
                            }
                        }
                    }
                } else {
//                    logger.info("date not existed " + line);
                }
            }
        }
    }

    @Test
    public void changeReplicaProfileRealtimeIndices() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices?h=index,health,status,rep");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        Date now = cal.getTime();

        for (String line : lines) {
            // mel_com_private_music_st_profile_realtime_2018040316 green open   0
            // mel_com_private_music_st_profile_realtime_2018040216       close

            if (line.startsWith("mel_com_private_music_st_profile_realtime_") || line.startsWith("mel_com_private_pv_profile_realtime") || line.startsWith("mel_com_private_dl_profile_realtime")) {
                String[] lineTokens = line.split("( )+");
                if (lineTokens.length == 4 && !"red".equals(lineTokens[1]) && !"close".equals(lineTokens[1]) && "0".equals(lineTokens[3])) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
                    String indexCreationHourStr = StringUtils.substringAfterLast(lineTokens[0], "_");

                    try {
                        Date indexCreationHour = sdf.parse(indexCreationHourStr);
                        if (indexCreationHour.before(now)) {
                            logger.info("target index: " + line);
                            client.admin().indices().prepareUpdateSettings(lineTokens[0]).setSettings(Settings.builder().put("number_of_replicas", "1").build()).get();

                            logger.info(lineTokens[0] + " number_of_replicas changed.");
                        } else {
//							logger.info("not target index: " + line);
                        }
                    } catch (ParseException e) {
                        logger.info("Parse Exception Occur: " + line);
                    }
                } else {
//					logger.info("not target index: " + line);
                }
            }
        }
    }

    @Test
    public void deleteYellowIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");

        int redOpenCnt = 0;
        for (String line : lines) {
            if (line.replace(" ", "").contains("yellowopen")) {
                redOpenCnt++;
            }
        }

        int i = 1;
        for (String line : lines) {
            String status = line.split(" ")[0].replace(" ", "");
//            System.out.println("line = " + line + " status = " + status);
            if (status.equals("yellow")) {
                String indexName = getYellowIndexName(line);
                System.out.println("delete indexName (" + i++ + "/" + redOpenCnt + ")= " + indexName);
                client.admin().indices().prepareDelete(indexName).get();
            }
        }
    }

    @Test
    public void deleteRedIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");

        int redOpenCnt = 0;
        for (String line : lines) {
            if (line.replace(" ", "").contains("redopen")) {
                redOpenCnt++;
            }
        }

        int i = 1;
        for (String line : lines) {
            String status = line.split(" ")[0].replace(" ", "");
//            System.out.println("line = " + line + " status = " + status);
            if (status.equals("red")) {
                String indexName = getIndexName(line);
                System.out.println("delete indexName (" + i++ + "/" + redOpenCnt + ")= " + indexName);
                client.admin().indices().prepareDelete(indexName).get();
            }
        }
    }

    private String getIndexName(String line) {
        char[] carr = {'r', 'e', 'd', 'o', 'p', 'e', 'n'};

        int cIndex = 0;
        int index = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (cIndex == carr.length) {
                if (c != ' ') {
                    index = i;
                    break;
                }
            }

            if (cIndex < carr.length && carr[cIndex] == c) {
                cIndex++;
            }
        }

        String indexName = line.substring(index, line.length()).split(" ")[0];
        return indexName;
    }

    private String getYellowIndexName(String line) {
        char[] carr = {'y', 'e', 'l', 'l', 'o', 'w', 'o', 'p', 'e', 'n'};

        int cIndex = 0;
        int index = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (cIndex == carr.length) {
                if (c != ' ') {
                    index = i;
                    break;
                }
            }

            if (cIndex < carr.length && carr[cIndex] == c) {
                cIndex++;
            }
        }

        String indexName = line.substring(index, line.length()).split(" ")[0];
        return indexName;
    }

    private String getGreenIndexName(String line) {
        char[] carr = {'g', 'r', 'e', 'e', 'n', 'o', 'p', 'e', 'n'};

        int cIndex = 0;
        int index = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (cIndex == carr.length) {
                if (c != ' ') {
                    index = i;
                    break;
                }
            }

            if (cIndex < carr.length && carr[cIndex] == c) {
                cIndex++;
            }
        }

        String indexName = line.substring(index, line.length()).split(" ")[0];
        return indexName;
    }

    @Test
    public void changeRefreshTimeInOldIndices() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if (!"red".equals(status)) {
                LocalDate date = LocalDate.now();
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
                String dateString = date.toString(fmt);
                if (!line.contains(dateString)) {
                    String indexName = null;
                    if (line.indexOf("mel_") > 0) {
                        indexName = line.substring(line.indexOf("mel_"), line.length()).split(" ")[0];
                    } else if (line.indexOf("mtk_") > 0) {
                        indexName = line.substring(line.indexOf("mtk_"), line.length()).split(" ")[0];
                    }

                    if (indexName != null) {
                        GetSettingsResponse gsr = client.admin().indices().prepareGetSettings(indexName).get();
                        String refreshInterval = gsr.getSetting(indexName, "index.refresh_interval");

                        if (!"-1".equals(refreshInterval)) { // refresh 가 꺼진게 아니거나 레플리카가 없으면
                            client.admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                                    .put("refresh_interval", "-1")
                                    .build()).get();
                            logger.info(cnt++ + " " + indexName + " refresh changed.(" + refreshInterval + " to -1)");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void changeRefreshTimeAndReplicaInOldIndices() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if (!"red".equals(status)) {
                LocalDate date = LocalDate.now();
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
                String dateString = date.toString(fmt);
                if (!line.contains(dateString)) {
                    String indexName = null;
                    if (line.indexOf("mel_") > 0) {
                        indexName = line.substring(line.indexOf("mel_"), line.length()).split(" ")[0];
                    } else if (line.indexOf("mtk_") > 0) {
                        indexName = line.substring(line.indexOf("mtk_"), line.length()).split(" ")[0];
                    }

                    if (indexName != null) {
                        GetSettingsResponse gsr = client.admin().indices().prepareGetSettings(indexName).get();
                        String refreshInterval = gsr.getSetting(indexName, "index.refresh_interval");
                        String replica = gsr.getSetting(indexName, "index.number_of_replicas");

                        if (!"-1".equals(refreshInterval) || "0".equals(replica)) { // refresh 가 꺼진게 아니거나 레플리카가 없으면
                            client.admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
                                    .put("refresh_interval", "-1")
                                    .put("number_of_replicas", "1").build()).get();
                            logger.info(cnt++ + " " + indexName + " refresh and replica changed.");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void changeReplicaInOldIndices() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            if (!"red".equals(status)) {
                LocalDate date = LocalDate.now();
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
                String dateString = date.toString(fmt);
                if (!line.contains(dateString)) {
                    String indexName = null;
                    if (line.indexOf("mel_") > 0) {
                        indexName = line.substring(line.indexOf("mel_"), line.length()).split(" ")[0];
                    } else if (line.indexOf("mtk_") > 0) {
                        indexName = line.substring(line.indexOf("mtk_"), line.length()).split(" ")[0];
                    }
                    if (indexName != null) {
                        GetSettingsResponse gsr = client.admin().indices().prepareGetSettings(indexName).get();
                        String replica = gsr.getSetting(indexName, "index.number_of_replicas");

                        if ("0".equals(replica)) {
                            client.admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder().put("number_of_replicas", "1").build()).get();
                            logger.info(cnt++ + " " + indexName + " number_of_replicas changed.");
                        }
                    }
                }
            }
        }
    }

    @Test
    public void openNecessaryIndicies() {
        OpenIndexResponse r = client.admin().indices().prepareOpen("*st_profile" + "*").get();
        r = client.admin().indices().prepareOpen("*pv_profile" + "*").get();
        r = client.admin().indices().prepareOpen("*dl_profile" + "*").get();
        r = client.admin().indices().prepareOpen("*join_profile" + "*").get();
        r = client.admin().indices().prepareOpen("*cncl_profile" + "*").get();
        r = client.admin().indices().prepareOpen("*user_profile" + "*").get();
        r = client.admin().indices().prepareOpen("map" + "*").get();
        r = client.admin().indices().prepareOpen(".kibana" + "*").get();
        r = client.admin().indices().prepareOpen("esc" + "*").get();

        DateTime dt = new DateTime();
        r = client.admin().indices().prepareOpen(dt.getYear() + dt.getMonthOfYear() + dt.getDayOfMonth() + "*").get();
    }

    @Test
    public void empty() {
        OpenIndexResponse r = null;
        r = client.admin().indices().prepareOpen("mel_com_private_sns_post" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_sns_incoming" + "*").get();
    }

    @Test
    public void name() {
        final String path = "/user/hive/warehouse/sys_matrix.db/mel_com_private_pv_realtime_member/log_date=2017071011";
        String[] pathDetail = StringUtils.split(path, "/=");
        System.out.println(pathDetail.length);
    }

    @Test
    public void openServiceStatsIndicies() {
        OpenIndexResponse r = client.admin().indices().prepareOpen("mel_com_private_pv_member" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_cl_member" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_dl" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_music_st_member" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_accum_mem_act" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_like_accum" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_like_conts" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_usell_dl" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_preview" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_mv_view" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_sns_post" + "*").get();
        r = client.admin().indices().prepareOpen("mel_com_private_sns_incoming" + "*").get();
    }

    @Test
    public void list() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        Map<String, Integer> map = new HashMap<>();
        for (String line : lines) {
            if (line.contains("mel_com_private_sns_incoming")
                    || line.contains("mel_com_private_sns_post")
                    || line.contains("mel_com_private_mv_view")
                    || line.contains("mel_com_private_preview")
                    || line.contains("mel_com_private_usell_dl")
                    || line.contains("mel_com_private_like_conts")
                    || line.contains("mel_com_private_like_accum")
                    || line.contains("mel_com_private_accum_mem_act")
                    || line.contains("mel_com_private_music_st_member")
                    || line.contains("mel_com_private_pv_member")
                    || line.contains("mel_com_private_cl_member")
                    || line.contains("mel_com_private_dl")
                    || line.contains("profile")
                    || line.contains(".kibana")
                    || line.contains("map")
                    || line.contains(".monitoring")
                    || line.contains(".watch")
                    || line.contains(".triggered_watches")
                    || line.contains("esc_")
                    || line.contains(".security")
                    || line.contains("metricbeat")
            ) {
                continue;
            }
            String[] tab = line.split("close ");
//            System.out.println(line);
            if (tab.length >= 2) {
                String indexName = tab[1].split(", ")[0].split("2018")[0];
                if (map.containsKey(indexName)) {
                    map.put(indexName, map.get(indexName) + 1);
                } else {
                    map.put(indexName, 1);
                }
            }
        }

        for (String key : map.keySet()) {
            System.out.println(key + " " + map.get(key));
        }
    }

    @Test
    public void deleteIndex() throws Exception {
        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            logger.info(line);
            if (line.split("red   open  ").length > 1) {
                String redIndex = line.split("red   open  ")[1].split(" ")[0];
                client.admin().indices().prepareDelete(redIndex).get();
                logger.info(redIndex + " deleted.");
            }
        }
    }

    @Test
    public void testBackup() throws IOException {
//        restClient.performRequest("PUT", "/_snapshot/my_backup/" + "total_backup");
    }

    @Test
    public void closeDailyOldIndices() throws Exception {

        String[] arrIndexContains = new String[]{
                "profile_daily",
                "cncl_profile",
                "join_profile",
                "mel_com_private_usell_dl",
                "mel_com_private_preview",
                "mel_com_private_sns_post",
                "mel_com_private_sns_incoming",
                "mel_com_private_mv_view",
                "mel_com_private_like_accum",
                "mel_com_private_like_conts",
                "mel_com_private_accum_mem_act",
                "mel_com_private_mem_act",
                "mel_com_private_music_st_member",
                "mel_com_private_dl",
                "mel_com_private_pv_member",
                "mel_com_private_cl_member"
        };


        Response r = restClient.performRequest("GET", "/_cat/indices");
        String body = EntityUtils.toString(r.getEntity());
        String[] lines = body.split("\n");
        int cnt = 0;
        for (String line : lines) {
            String status = line.split(" ")[0];
            String status2 = line.split(" ")[1];
            if (!"red".equals(status) && line.contains("") && "open".equals(status2)) {

                LocalDate date = LocalDate.now();
                DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
                String dateString = date.toString(fmt);
                if (!line.contains(dateString)) {
                    String indexName = null;
                    if (line.indexOf("mel_") > 0) {
                        indexName = line.substring(line.indexOf("mel_"), line.length()).split(" ")[0];
                    } else if (line.indexOf("mtk_") > 0) {
                        indexName = line.substring(line.indexOf("mtk_"), line.length()).split(" ")[0];
                    }

                    if (indexName != null) {
                        for (String arrIndexContain : arrIndexContains) {
                            if (indexName.contains(arrIndexContain) && !indexName.contains("realtime")) {
                                try {
                                    DateTimeFormatter fmt2 = DateTimeFormat.forPattern("yyyyMMdd");

                                    String dateFromIndex = indexName.split("_")[indexName.split("_").length - 1];

                                    DateTime dateTimeFromIndex = fmt2.parseDateTime(dateFromIndex);

                                    DateTime curDateTime = new DateTime();
                                    int agingDays = Days.daysBetween(dateTimeFromIndex.toLocalDate(), curDateTime.toLocalDate()).getDays();

                                    if (agingDays > 45) {
                                        client.admin().indices().prepareClose(indexName).get();
                                        logger.info(cnt++ + " " + indexName + " closed, agingDays = " + agingDays + "     " + status2);
                                    }
                                } catch (Exception e) {
                                    logger.info("exception with " + indexName + e.getMessage());
                                }
                            }
                        }
                    } else {
//                    logger.info("date not existed " + line);
                    }
                }
            }

        }
    }

    @Test
    public void testConnection() throws SQLException {
        // URL parameters
        String url = "jdbc:presto://mpcollect01:18080/hive/sys_matrix";
        Properties properties = new Properties();
        properties.setProperty("user", "presto");
        properties.setProperty("SSL", "false");

        Connection connection = DriverManager.getConnection(url, properties);

        Statement s = connection.createStatement();
//        boolean r = s.execute("show databases");

//        boolean r = s.execute("select count(1) as temp from  stats.sys_matrix.mel_com_private_user_profile");
//        System.out.println(s.getResultSet().first());
        ResultSet rs = s.executeQuery("select artist_id, album_id, song_id from  stats.ma_conts.o_partr_song_day limit 10");
        while(rs.next()) {
            logger.info("#### rs : " + rs.getString(1));
        }
    }
}
