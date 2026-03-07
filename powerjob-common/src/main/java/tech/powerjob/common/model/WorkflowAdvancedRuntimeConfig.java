package tech.powerjob.common.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * 工作流运行时高级配置
 *
 * @author Echo009
 * @since 2025/3/7
 */
@Getter
@Setter
@ToString
@Accessors(chain = true)
public class WorkflowAdvancedRuntimeConfig {

    /**
     * 超过最大实例数时的处理策略 {@link tech.powerjob.common.enums.InstanceLimitStrategy}
     * 默认 FAIL（直接失败），保持前向兼容
     */
    private Integer instanceLimitStrategy;

}
