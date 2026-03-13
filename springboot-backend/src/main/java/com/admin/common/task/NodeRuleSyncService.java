package com.admin.common.task;

import com.admin.common.dto.GostDto;
import com.admin.common.lang.R;
import com.admin.common.utils.GostUtil;
import com.admin.entity.Forward;
import com.admin.entity.SpeedLimit;
import com.admin.entity.Tunnel;
import com.admin.service.ForwardService;
import com.admin.service.SpeedLimitService;
import com.admin.service.TunnelService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class NodeRuleSyncService {

    private static final int FORWARD_STATUS_ACTIVE = 1;
    private static final int SPEED_LIMIT_STATUS_ACTIVE = 1;
    private static final int TUNNEL_TYPE_PORT_FORWARD = 1;
    private static final String GOST_SUCCESS_MSG = "OK";
    private static final String GOST_NOT_FOUND_MSG = "not found";
    private static final long[] DEFAULT_RETRY_DELAYS_MS = {0L, 5000L, 15000L};

    @Resource
    @Lazy
    private TunnelService tunnelService;

    @Resource
    @Lazy
    private ForwardService forwardService;

    @Resource
    @Lazy
    private SpeedLimitService speedLimitService;

    @Async
    public void scheduleNodeRuleSync(Long nodeId, long... delaysMs) {
        if (nodeId == null) {
            return;
        }

        long[] retryPlan = (delaysMs == null || delaysMs.length == 0) ? DEFAULT_RETRY_DELAYS_MS : delaysMs;

        for (int attempt = 0; attempt < retryPlan.length; attempt++) {
            long delayMs = retryPlan[attempt];

            if (delayMs > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("节点 {} 规则补发任务被中断", nodeId);
                    return;
                }
            }

            try {
                int limiterCount = syncLimiters(nodeId);
                int forwardCount = syncForwards(nodeId);
                log.info("节点 {} 第 {} 次规则补发完成，限速器 {} 条，转发 {} 条", nodeId, attempt + 1, limiterCount, forwardCount);
            } catch (Exception e) {
                log.error("节点 {} 第 {} 次规则补发失败: {}", nodeId, attempt + 1, e.getMessage(), e);
            }
        }
    }

    private int syncLimiters(Long nodeId) {
        List<Tunnel> inTunnels = tunnelService.list(new QueryWrapper<Tunnel>().eq("in_node_id", nodeId));
        if (inTunnels.isEmpty()) {
            return 0;
        }

        Set<Long> tunnelIds = inTunnels.stream()
                .map(Tunnel::getId)
                .collect(Collectors.toSet());

        List<SpeedLimit> speedLimits = speedLimitService.list(
                new QueryWrapper<SpeedLimit>()
                        .in("tunnel_id", tunnelIds)
                        .eq("status", SPEED_LIMIT_STATUS_ACTIVE)
        );

        int syncedCount = 0;
        for (SpeedLimit speedLimit : speedLimits) {
            if (speedLimit.getId() == null || speedLimit.getSpeed() == null) {
                continue;
            }

            String speedInMBps = convertBitsToMBps(speedLimit.getSpeed());
            GostDto result = GostUtil.UpdateLimiters(nodeId, speedLimit.getId(), speedInMBps);
            if (result != null && result.getMsg() != null && result.getMsg().contains(GOST_NOT_FOUND_MSG)) {
                result = GostUtil.AddLimiters(nodeId, speedLimit.getId(), speedInMBps);
            }

            if (isGostSuccess(result)) {
                syncedCount++;
            } else {
                log.warn("节点 {} 同步限速器 {} 失败: {}", nodeId, speedLimit.getId(), result != null ? result.getMsg() : "未知错误");
            }
        }

        return syncedCount;
    }

    private int syncForwards(Long nodeId) {
        List<Tunnel> inTunnels = tunnelService.list(new QueryWrapper<Tunnel>().eq("in_node_id", nodeId));
        List<Tunnel> outTunnels = tunnelService.list(new QueryWrapper<Tunnel>().eq("out_node_id", nodeId));

        Set<Long> syncedForwardIds = new HashSet<>();
        int syncedCount = 0;

        syncedCount += syncForwardBatch(nodeId, inTunnels, syncedForwardIds, false);
        syncedCount += syncForwardBatch(nodeId, outTunnels, syncedForwardIds, true);

        return syncedCount;
    }

    private int syncForwardBatch(Long nodeId, List<Tunnel> tunnels, Set<Long> syncedForwardIds, boolean outNodeBatch) {
        int syncedCount = 0;

        for (Tunnel tunnel : tunnels) {
            if (outNodeBatch && Objects.equals(tunnel.getType(), TUNNEL_TYPE_PORT_FORWARD)) {
                continue;
            }

            List<Forward> forwards = forwardService.list(
                    new QueryWrapper<Forward>()
                            .eq("tunnel_id", tunnel.getId())
                            .eq("status", FORWARD_STATUS_ACTIVE)
            );

            for (Forward forward : forwards) {
                if (!syncedForwardIds.add(forward.getId())) {
                    continue;
                }

                R syncResult = forwardService.updateForwardA(forward);
                if (syncResult.getCode() == 0) {
                    syncedCount++;
                } else {
                    log.warn("节点 {} 同步转发规则 {} 失败: {}", nodeId, forward.getId(), syncResult.getMsg());
                }
            }
        }

        return syncedCount;
    }

    private boolean isGostSuccess(GostDto result) {
        return result != null && Objects.equals(result.getMsg(), GOST_SUCCESS_MSG);
    }

    private String convertBitsToMBps(Integer speedInBits) {
        BigDecimal speedInMBps = BigDecimal.valueOf(speedInBits)
                .divide(BigDecimal.valueOf(8), 1, RoundingMode.HALF_UP);
        return speedInMBps.toPlainString();
    }
}