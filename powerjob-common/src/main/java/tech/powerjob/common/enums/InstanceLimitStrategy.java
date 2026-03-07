package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 实例数量超限时的处理策略
 *
 * @author Echo009
 * @since 2025/3/7
 */
@Getter
@AllArgsConstructor
public enum InstanceLimitStrategy {

    /**
     * 直接失败（默认，保持前向兼容）
     */
    FAIL(0, "直接失败"),
    /**
     * 排队等待，由后台任务自动重试
     */
    WAIT(1, "排队等待");

    private final int v;
    private final String des;

    /**
     * 解析策略，默认返回 FAIL（保持前向兼容）
     *
     * @param v 策略值
     * @return 策略枚举
     */
    public static InstanceLimitStrategy of(Integer v) {
        if (v == null) {
            return FAIL;
        }
        for (InstanceLimitStrategy strategy : values()) {
            if (v == strategy.v) {
                return strategy;
            }
        }
        return FAIL;
    }
}
