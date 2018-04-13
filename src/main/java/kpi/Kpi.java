package kpi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * @Author: JF Han
 * @Date: Created in 9:17 2018/4/4
 * @Desc:
 */
public class Kpi {
    /**
     * 客户端的ip地址
     */
    private String remoteAddr;
    /**
     * 客户端用户名称,忽略属性"-"
     */
    private String remoteUser;
    /**
     * 访问时间与时区
     */
    private String timeLocal;
    /**
     * 请求的url与http协议
     */
    private String request;
    /**
     * 请求状态,成功:200
     */
    private String status;
    /**
     * 发送给客户端文件主题内容大小
     */
    private String bodyByteSent;
    /**
     * 跳转前的页面url
     */
    private String httpReferer;
    /**
     * 客户浏览器的相关信息
     */
    private String httpUserAgent;
    /**
     * 判断数据是否合法
     */
    private boolean valid = true;

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getTimeLocal() {
        return timeLocal;
    }

    public Date getTimeLocalDate() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
        return df.parse(timeLocal);
    }

    public String getTimeLocalDateHour() throws ParseException{
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
        return df.format(getTimeLocalDate());
    }

    public void setTimeLocal(String timeLocal) {
        this.timeLocal = timeLocal;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getBodyByteSent() {
        return bodyByteSent;
    }

    public void setBodyByteSent(String bodyByteSent) {
        this.bodyByteSent = bodyByteSent;
    }

    public String getHttpReferer() {
        return httpReferer;
    }

    public String getHttpRefererDomain(){
        if (httpReferer.length() < 8){
            return httpReferer;
        }

        String str = httpReferer.replace("\"","").replace("http://","").replace("https://","");
        return str.indexOf("/") > 0 ? str.substring(0,str.indexOf("/")) : str;
    }

    public void setHttpReferer(String httpReferer) {
        this.httpReferer = httpReferer;
    }

    public String getHttpUserAgent() {
        return httpUserAgent;
    }

    public void setHttpUserAgent(String httpUserAgent) {
        this.httpUserAgent = httpUserAgent;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    private static Kpi parser(String line){
        System.out.println("\n" + line + "\n");

        Kpi kpi = new Kpi();
        String[] arr = line.split(" ");
        if (arr.length > 11){
            kpi.setRemoteAddr(arr[0]);
            kpi.setRemoteUser(arr[1]);
            kpi.setTimeLocal(arr[3].substring(1));
            kpi.setRequest(arr[6]);
            kpi.setStatus(arr[8]);
            kpi.setBodyByteSent(arr[9]);
            kpi.setHttpReferer(arr[10]);

            if (arr.length > 12){
                kpi.setHttpUserAgent(arr[11] + " " + arr[12]);
            } else {
                kpi.setHttpUserAgent(arr[11]);
            }

            //大于400,HTTP错误
            if (Integer.parseInt(kpi.getStatus()) >= 400){
                kpi.setValid(false);
            }
        } else {
            kpi.setValid(false);
        }

        return kpi;
    }

    public static Kpi filterPvs(String line){

        Kpi kpi = parser(line);
        Set<String> pages = new HashSet<>();
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if (!pages.contains(kpi.getRequest())) {
            kpi.setValid(false);
        }
        return kpi;
    }

    @Override
    public String toString() {

        return "valid:" + valid +
                "\nremoteAddr:" + remoteAddr +
                "\nremoteUser:" + remoteUser +
                "\ntimeLocal:" + timeLocal +
                "\nrequest:" + request +
                "\nstatus:" + status +
                "\nbodyByteSent:" + bodyByteSent +
                "\nhttpReferer:" + httpReferer +
                "\nhttpUserAgent:" + httpUserAgent;
    }

    public static void main(String...args){
        String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
        System.out.println("\n" + line + "\n");
        Kpi kpi = new Kpi();
        String[] arr = line.split(" ");

        kpi.setRemoteAddr(arr[0]);
        kpi.setRemoteUser(arr[1]);
        kpi.setTimeLocal(arr[3].substring(1));
        kpi.setRequest(arr[6]);
        kpi.setStatus(arr[8]);
        kpi.setBodyByteSent(arr[9]);
        kpi.setHttpReferer(arr[10]);
        kpi.setHttpUserAgent(arr[11] + " " + arr[12]);

        System.out.println(kpi);

        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
            System.out.println(dateFormat.format(kpi.getTimeLocalDate()));
            System.out.println(kpi.getTimeLocalDateHour());
            System.out.println(kpi.getHttpRefererDomain());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
