package com.admin.common.task;

import com.admin.common.dto.ConfigItem;
import com.admin.common.dto.GostConfigDto;
import com.admin.common.utils.GostUtil;
import com.admin.entity.Forward;
import com.admin.entity.Node;
import com.admin.entity.SpeedLimit;
import com.admin.entity.Tunnel;
import com.admin.entity.UserTunnel;
import com.admin.service.ForwardService;
import com.admin.service.NodeService;
import com.admin.service.SpeedLimitService;
import com.admin.service.TunnelService;
import com.admin.service.UserTunnelService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class CheckGostConfigAsync {

    private static final int FORWARD_STATUS_ACTIVE = 1;
    private static final int FORWARD_STATUS_ERROR = -1;
    private static final int SPEED_LIMIT_STATUS_ACTIVE = 1;
    private static final int TUNNEL_TYPE_TUNNEL_FORWARD = 2;
    private static final long REPAIR_SYNC_COOLDOWN_MS = 120_000L;
    private static final ConcurrentHashMap<Long, Long> LAST_REPAIR_SYNC_AT = new ConcurrentHashMap<>();

    @Resource
    private NodeService nodeService;

    @Resource
    @Lazy
    private ForwardService forwardService;

    @Resource
    @Lazy
    private SpeedLimitService speedLimitService;

    @Resource
    @Lazy
    private TunnelService tunnelService;

    @Resource
    @Lazy
    private UserTunnelService userTunnelService;

    @Resource
    private NodeRuleSyncService nodeRuleSyncService;

    @Async
    public void cleanNodeConfigs(String nodeId, GostConfigDto gostConfig) {
        Node node = nodeService.getById(nodeId);
        if (node == null) {
            return;
        }

        cleanOrphanedServices(gostConfig, node);
        cleanOrphanedChains(gostConfig, node);
        cleanOrphanedLimiters(gostConfig, node);
        triggerRepairSyncIfMissing(gostConfig, node);
    }

    private void cleanOrphanedServices(GostConfigDto gostConfig, Node node) {
        if (gostConfig.getServices() == null) {
            return;
        }

        for (ConfigItem service : gostConfig.getServices()) {
            safeExecute(() -> {
                if (Objects.equals(service.getName(), "web_api")) {
                    return;
                }

                String[] serviceIds = parseServiceName(service.getName());
                if (serviceIds.length != 4) {
                    return;
                }

                String forwardId = serviceIds[0];
                String userId = serviceIds[1];
                String userTunnelId = serviceIds[2];
                String type = serviceIds[3];
                Forward forward = forwardService.getById(forwardId);
                if (forward != null) {
                    return;
                }

                if (Objects.equals(type, "tcp")) {
                    log.info("删除孤立服务 {} (节点: {})", service.getName(), node.getId());
                    GostUtil.DeleteService(node.getId(), forwardId + "_" + userId + "_" + userTunnelId);
                    return;
                }

                if (Objects.equals(type, "tls")) {
                    log.info("删除孤立远程服务 {} (节点: {})", service.getName(), node.getId());
                    GostUtil.DeleteRemoteService(node.getId(), forwardId + "_" + userId + "_" + userTunnelId);
                }
            }, "清理服务 " + service.getName());
        }
    }

    private void cleanOrphanedChains(GostConfigDto gostConfig, Node node) {
        if (gostConfig.getChains() == null) {
            return;
        }

        for (ConfigItem chain : gostConfig.getChains()) {
            safeExecute(() -> {
                String[] serviceIds = parseServiceName(chain.getName());
                if (serviceIds.length != 4 || !Objects.equals(serviceIds[3], "chains")) {
                    return;
                }

                Forward forward = forwardService.getById(serviceIds[0]);
                if (forward == null) {
                    log.info("删除孤立链 {} (节点: {})", chain.getName(), node.getId());
                    GostUtil.DeleteChains(node.getId(), serviceIds[0] + "_" + serviceIds[1] + "_" + serviceIds[2]);
                }
            }, "清理链 " + chain.getName());
        }
    }

    private void cleanOrphanedLimiters(GostConfigDto gostConfig, Node node) {
        if (gostConfig.getLimiters() == null) {
            return;
        }

        for (ConfigItem limiter : gostConfig.getLimiters()) {
            safeExecute(() -> {
                SpeedLimit speedLimit = speedLimitService.getById(limiter.getName());
                if (speedLimit == null) {
                    log.info("删除孤立限速器 {} (节点: {})", limiter.getName(), node.getId());
                    GostUtil.DeleteLimiters(node.getId(), Long.parseLong(limiter.getName()));
                }
            }, "清理限速器 " + limiter.getName());
        }
    }

    private void triggerRepairSyncIfMissing(GostConfigDto gostConfig, Node node) {
        Set<String> currentServices = toNameSet(gostConfig.getServices());
        Set<String> currentChains = toNameSet(gostConfig.getChains());
        Set<String> currentLimiters = toNameSet(gostConfig.getLimiters());

        Set<String> missingServices = findMissingNames(collectExpectedServiceNames(node.getId()), currentServices);
        Set<String> missingChains = findMissingNames(collectExpectedChainNames(node.getId()), currentChains);
        Set<String> missingLimiters = findMissingNames(collectExpectedLimiterNames(node.getId()), currentLimiters);

        if (missingServices.isEmpty() && missingChains.isEmpty() && missingLimiters.isEmpty()) {
            return;
        }

        if (!shouldTriggerRepairSync(node.getId())) {
            log.info("节点 {} 仍有缺失配置，但仍在冷却窗口内，跳过补发触发", node.getId());
            return;
        }

        log.info(
                "节点 {} 检测到缺失配置，触发差异补发 services={}, chains={}, limiters={}",
                node.getId(),
                missingServices,
                missingChains,
                missingLimiters
        );
        nodeRuleSyncService.scheduleNodeRuleSync(node.getId(), 1000L, 5000L, 15000L, 30000L, 60000L);
    }

    private boolean shouldTriggerRepairSync(Long nodeId) {
        long now = System.currentTimeMillis();
        Long lastTriggerAt = LAST_REPAIR_SYNC_AT.get(nodeId);
        if (lastTriggerAt != null && now - lastTriggerAt < REPAIR_SYNC_COOLDOWN_MS) {
            return false;
        }
        LAST_REPAIR_SYNC_AT.put(nodeId, now);
        return true;
    }

    private Set<String> collectExpectedServiceNames(Long nodeId) {
        Set<String> expectedNames = new HashSet<>();
        Map<String, Integer> userTunnelCache = new HashMap<>();

        List<Tunnel> inTunnels = tunnelService.list(new QueryWrapper<Tunnel>().eq("in_node_id", nodeId));
        for (Tunnel tunnel : inTunnels) {
            for (Forward forward : listSyncableForwards(tunnel.getId())) {
                String baseName = buildServiceBaseName(forward, tunnel, userTunnelCache);
                expectedNames.add(baseName + "_tcp");
                expectedNames.add(baseName + "_udp");
            }
        }

        List<Tunnel> outTunnels = tunnelService.list(
                new QueryWrapper<Tunnel>()
                        .eq("out_node_id", nodeId)
                        .eq("type", TUNNEL_TYPE_TUNNEL_FORWARD)
        );
        for (Tunnel tunnel : outTunnels) {
            for (Forward forward : listSyncableForwards(tunnel.getId())) {
                String baseName = buildServiceBaseName(forward, tunnel, userTunnelCache);
                expectedNames.add(baseName + "_tls");
            }
        }

        return expectedNames;
    }

    private Set<String> collectExpectedChainNames(Long nodeId) {
        Set<String> expectedNames = new HashSet<>();
        Map<String, Integer> userTunnelCache = new HashMap<>();

        List<Tunnel> inTunnels = tunnelService.list(
                new QueryWrapper<Tunnel>()
                        .eq("in_node_id", nodeId)
                        .eq("type", TUNNEL_TYPE_TUNNEL_FORWARD)
        );
        for (Tunnel tunnel : inTunnels) {
            for (Forward forward : listSyncableForwards(tunnel.getId())) {
                String baseName = buildServiceBaseName(forward, tunnel, userTunnelCache);
                expectedNames.add(baseName + "_chains");
            }
        }

        return expectedNames;
    }

    private Set<String> collectExpectedLimiterNames(Long nodeId) {
        Set<String> expectedNames = new HashSet<>();
        List<Tunnel> inTunnels = tunnelService.list(new QueryWrapper<Tunnel>().eq("in_node_id", nodeId));
        if (inTunnels.isEmpty()) {
            return expectedNames;
        }

        List<Long> tunnelIds = new ArrayList<>();
        for (Tunnel tunnel : inTunnels) {
            tunnelIds.add(tunnel.getId());
        }

        List<SpeedLimit> speedLimits = speedLimitService.list(
                new QueryWrapper<SpeedLimit>()
                        .in("tunnel_id", tunnelIds)
                        .eq("status", SPEED_LIMIT_STATUS_ACTIVE)
        );
        for (SpeedLimit speedLimit : speedLimits) {
            if (speedLimit.getId() != null) {
                expectedNames.add(String.valueOf(speedLimit.getId()));
            }
        }

        return expectedNames;
    }

    private List<Forward> listSyncableForwards(Long tunnelId) {
        return forwardService.list(
                new QueryWrapper<Forward>()
                        .eq("tunnel_id", tunnelId)
                        .in("status", FORWARD_STATUS_ACTIVE, FORWARD_STATUS_ERROR)
        );
    }

    private String buildServiceBaseName(Forward forward, Tunnel tunnel, Map<String, Integer> userTunnelCache) {
        String cacheKey = forward.getUserId() + ":" + tunnel.getId();
        Integer userTunnelId = userTunnelCache.computeIfAbsent(cacheKey, ignored -> {
            UserTunnel userTunnel = userTunnelService.getOne(
                    new QueryWrapper<UserTunnel>()
                            .eq("user_id", forward.getUserId())
                            .eq("tunnel_id", tunnel.getId())
            );
            return userTunnel != null ? userTunnel.getId() : 0;
        });
        return forward.getId() + "_" + forward.getUserId() + "_" + userTunnelId;
    }

    private Set<String> toNameSet(List<ConfigItem> items) {
        Set<String> names = new HashSet<>();
        if (items == null) {
            return names;
        }

        for (ConfigItem item : items) {
            if (item != null && item.getName() != null) {
                names.add(item.getName());
            }
        }
        return names;
    }

    private Set<String> findMissingNames(Set<String> expectedNames, Set<String> currentNames) {
        Set<String> missingNames = new HashSet<>(expectedNames);
        missingNames.removeAll(currentNames);
        return missingNames;
    }

    private void safeExecute(Runnable operation, String operationDesc) {
        try {
            operation.run();
        } catch (Exception e) {
            log.info("执行操作失败: {}", operationDesc, e);
        }
    }

    private String[] parseServiceName(String serviceName) {
        return serviceName.split("_");
    }
}
