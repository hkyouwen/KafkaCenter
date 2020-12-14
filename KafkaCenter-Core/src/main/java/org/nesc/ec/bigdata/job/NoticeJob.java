package org.nesc.ec.bigdata.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.nesc.ec.bigdata.common.model.OffsetInfo;
import org.nesc.ec.bigdata.config.InitConfig;
import org.nesc.ec.bigdata.constant.BrokerConfig;
import org.nesc.ec.bigdata.constant.Constants;
import org.nesc.ec.bigdata.constant.TopicConfig;
import org.nesc.ec.bigdata.model.AlertGoup;
import org.nesc.ec.bigdata.model.MonitorNoticeInfo;
import org.nesc.ec.bigdata.service.AlertService;
import org.nesc.ec.bigdata.service.AlertaService;
import org.nesc.ec.bigdata.service.EmailService;
import org.nesc.ec.bigdata.service.TopicInfoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author Truman.P.Du
 * @date 2020/08/06
 * @description
 */
@Component
public class NoticeJob {

    private static final Logger LOG = LoggerFactory.getLogger(NoticeJob.class);

    @Autowired
    TopicInfoService topicInfoService;

    @Autowired
    AlertService alertService;
    @Autowired
    EmailService emailService;

    @Autowired
    RestTemplate restTemplate;
    @Autowired
    InitConfig initConfig;

    @Autowired
    AlertaService alertaService;

    public static BlockingQueue<MonitorNoticeInfo> alertQueue = new ArrayBlockingQueue<>(1000);
    public static Map<String, Long> cachedLastSendTime = new HashMap<>();

    /**
     * 通知线程，主要是发送监控告警邮件和webhook、alerta
     */
    public void runNoticeJob() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    MonitorNoticeInfo monitorNoticeInfo = alertQueue.take();
                    AlertGoup alertGroup = monitorNoticeInfo.getAlertGoup();
                    long dispause = Optional.ofNullable(alertGroup.getDispause()).orElse(0);
                    String key = alertGroup.getCluster().getId() + Constants.Symbol.VERTICAL_STR + alertGroup.getTopicName() + Constants.Symbol.VERTICAL_STR
                            + alertGroup.getConsummerGroup();
                    if (cachedLastSendTime.containsKey(key)) {
                        Long lastTime = cachedLastSendTime.get(key);
                        Long now = System.currentTimeMillis();

                        if(Constants.SendType.ALERTA.equalsIgnoreCase(monitorNoticeInfo.getSendType())){
                            if(alertaService.isSendToAlerta(monitorNoticeInfo,now,lastTime)){
                                notice(monitorNoticeInfo);
                            }
                        }else{
                            if ((now - lastTime) >= dispause * 60 * 1000) {
                                notice(monitorNoticeInfo);
                            }
                        }
                    } else {
                        notice(monitorNoticeInfo);
                    }
                } catch (Exception e) {
                    LOG.error("Run Notice job has error: ", e);
                }
            }
        });
        thread.start();
    }

    private void notice(MonitorNoticeInfo monitorNoticeInfo) throws Exception {
        if(Constants.SendType.ALL.equalsIgnoreCase(monitorNoticeInfo.getSendType())) {
            try {
                sendToEmailOrWebHook(monitorNoticeInfo);
            } catch (Exception e) {
                LOG.error("sendToEmailOrWebHook: monitorNoticeInfo {}",monitorNoticeInfo.toString(), e);
            }
        }
        // 判断是否启用alerta，如果开启即发送alerta
        alertaService.sendToAlerta(monitorNoticeInfo);
    }

    private void sendToEmailOrWebHook(MonitorNoticeInfo monitorNoticeInfo) throws Exception {
        AlertGoup alertGroup = monitorNoticeInfo.getAlertGoup();
        String concont = alertGroup.getCluster().getName()+Constants.Symbol.VERTICAL_STR +alertGroup.getTopicName()+
                Constants.Symbol.VERTICAL_STR +alertGroup.getConsummerGroup()+Constants.Symbol.VERTICAL_STR +alertGroup.getConsummerApi();
        String key = alertGroup.getCluster().getId() + Constants.Symbol.VERTICAL_STR + alertGroup.getTopicName() + Constants.Symbol.VERTICAL_STR
                + alertGroup.getConsummerGroup();
        cachedLastSendTime.put(key, System.currentTimeMillis());
        if(alertGroup.isEnable()) {
            if(StringUtils.isNotBlank(alertGroup.getMailTo()) ){
                generateEmailContentAndSend(monitorNoticeInfo,concont);
            }
            if (StringUtils.isNotBlank(monitorNoticeInfo.getAlertGoup().getWebhook())) {
                generateWebHookContentAndSend(monitorNoticeInfo,concont);
            }
        }

    }


    /**
     * 生成邮件内容并发送
     *
     * @param monitorNoticeInfo  监控实体
     * @throws Exception 抛出异常，上级捕获
     */
    private void generateEmailContentAndSend(MonitorNoticeInfo monitorNoticeInfo,String concont) throws Exception {
        try {
            // 获取邮件内容
            Map<String, Object> emailMap = alertService.getEmailAllMessage(monitorNoticeInfo);
            emailService.renderTemplateAndSend(emailMap.get("emailEntity"),emailMap.get("emailContent"),4);

        } catch (Exception e) {
            LOG.error("generateEmailContentAndSendException: "+concont, e);
        }
    }

    /**
     * 生成WebHook信息，并通知
     *
     * @param monitorNoticeInfo 监控实体
     * @throws Exception 抛出异常，上级捕获
     */
    private void generateWebHookContentAndSend(MonitorNoticeInfo monitorNoticeInfo,String concont) throws Exception {
        try {
            AlertGoup alertGoup = monitorNoticeInfo.getAlertGoup();

            String webHookUrl = alertGoup.getWebhook();
            List<Map<String, Object>> list = new ArrayList<>();
            for (OffsetInfo offsetInfo : monitorNoticeInfo.getOffsetInfos()) {
                Map<String, Object> map = new HashMap<>();
                map.put(BrokerConfig.GROUP, offsetInfo.getGroup());
                map.put(BrokerConfig.TOPIC, offsetInfo.getTopic());
                map.put(TopicConfig.PARTITIONS, offsetInfo.getPartition());
                map.put(TopicConfig.LAG, offsetInfo.getLag());
                map.put(Constants.KeyStr.COMSUMBER_API, offsetInfo.getConsumerMethod());
                list.add(map);
            }
            HttpHeaders headers = new HttpHeaders();
            headers.add(Constants.KeyStr.CONTENT_TYPE, Constants.KeyStr.APPLICATION_JSON);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(Constants.KeyStr.MESSAGE, list);
            jsonObject.put(Constants.KeyStr.CLUSTER, alertGoup.getCluster().getName());
            HttpEntity<JSONObject> httpEntity = new HttpEntity<>(jsonObject, headers);
            restTemplate.postForEntity(webHookUrl, httpEntity, String.class);
            LOG.info("generateWebHookContentAndSend..."+concont);
        } catch (Exception e) {
            throw new RuntimeException("generateWebHookContentAndSendException: ", e);
        }
    }


}
