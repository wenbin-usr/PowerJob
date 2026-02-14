package tech.powerjob.server.web.controller;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.common.response.AskResponse;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.common.utils.CollectionUtils;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.server.common.utils.TestUtils;
import tech.powerjob.server.core.alarm.AlarmCenter;
import tech.powerjob.server.core.alarm.module.JobInstanceAlarm;
import tech.powerjob.server.extension.alarm.AlarmTarget;
import tech.powerjob.server.remote.transporter.TransportService;

import jakarta.annotation.Resource;
import java.util.Map;

/**
 * 开发团队专用（或者 PRO 用户用来做自检也可以 lol）
 * 测试某些强依赖运行时环境的组件，如 Mail 告警等
 *
 * @author tjq
 * @since 2023/7/31
 */
@Slf4j
@RestController
@RequestMapping("/test")
public class TestController {

    @Value("${server.port}")
    private int port;

    @Resource
    private AlarmCenter alarmCenter;
    @Resource
    private TransportService transportService;

    @RequestMapping("/io")
    public Map<String, Object> io(@RequestBody Map<String, Object> input) {
        log.info("[TestController] input: {}", JsonUtils.toJSONString(input));
        return input;
    }

    @GetMapping("/check")
    public void check() {
        Map<String, Object> testConfig = TestUtils.fetchTestConfig();
        if (CollectionUtils.isEmpty(testConfig)) {
            log.info("[TestController] testConfig not exist, skip check!");
            return;
        }

        log.info("[TestController] testConfig: {}", JsonUtils.toJSONString(testConfig));

        testAlarmCenter();
    }

    @PostMapping("/transport")
    public Object testTransportService(@RequestBody Map<String, Object> params) throws ClassNotFoundException {
        String method = MapUtils.getString(params, "method");

        String protocol = MapUtils.getString(params, "protocol");
        Object url = MapUtils.getObject(params, "url");
        Object request = MapUtils.getObject(params, "request");
        String requestClassName = MapUtils.getString(params, "requestClassName");
        Class<? extends PowerSerializable> requestClz = (Class<? extends PowerSerializable>) Class.forName(requestClassName);

        if ("ask".equalsIgnoreCase(method)) {
            return transportService.ask(protocol, JsonUtils.toJavaObject(url, URL.class), JsonUtils.toJavaObject(request, requestClz) , AskResponse.class);
        }

        transportService.tell(protocol, JsonUtils.toJavaObject(url, URL.class), (PowerSerializable) request);
        return "TELL_SUCCESS";
    }

    void testAlarmCenter() {
        JobInstanceAlarm jobInstanceAlarm = new JobInstanceAlarm().setAppId(277).setJobId(1).setInstanceId(2)
                .setJobName("test-alarm").setJobParams("jobParams").setInstanceParams("instanceParams")
                .setExecuteType(1).setFinishedTime(System.currentTimeMillis());

        AlarmTarget target = new AlarmTarget().setName("ald").setPhone("208140").setExtra("extra")
                .setPhone(MapUtils.getString(TestUtils.fetchTestConfig(), TestUtils.KEY_PHONE_NUMBER))
                .setEmail("tjq@zju.edu.cn")
                .setWebHook(localUrlPath().concat("/test/io"));

        log.info("[TestController] start to testAlarmCenter, target: {}", target);
        alarmCenter.alarmFailed(jobInstanceAlarm, Lists.newArrayList(target));
    }

    private String localUrlPath() {
        return String.format("http://127.0.0.1:%d", port);
    }
}
