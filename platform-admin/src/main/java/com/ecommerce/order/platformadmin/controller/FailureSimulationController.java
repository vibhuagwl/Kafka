package com.ecommerce.order.platformadmin.controller;

import com.ecommerce.order.kafka.rebalance.InterviewRebalanceListener;
import com.ecommerce.order.kafka.threading.PartitionOrderedExecutor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Failure / ops simulation APIs for interview demos.
 *
 * <p>Broker / controller / leader failover are triggered via Docker scripts;
 * this API covers consumer pause/resume (back-pressure), listener crash simulation,
 * and rebalance counters.
 */
@RestController
@RequestMapping("/api/v1/ops")
public class FailureSimulationController {

    private final KafkaListenerEndpointRegistry registry;
    private final InterviewRebalanceListener rebalanceListener;
    private final PartitionOrderedExecutor executor;

    public FailureSimulationController(
            KafkaListenerEndpointRegistry registry,
            InterviewRebalanceListener rebalanceListener,
            PartitionOrderedExecutor executor
    ) {
        this.registry = registry;
        this.rebalanceListener = rebalanceListener;
        this.executor = executor;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rebalanceCount", rebalanceListener.rebalanceCount());
        out.put("workerPermits", executor.availablePermits());
        Map<String, Object> listeners = new LinkedHashMap<>();
        registry.getListenerContainers().forEach(c -> listeners.put(
                c.getListenerId(),
                Map.of(
                        "running", c.isRunning(),
                        "pauseRequested", c.isPauseRequested(),
                        "assignedPartitions", String.valueOf(c.getAssignedPartitions())
                )
        ));
        out.put("listeners", listeners);
        return out;
    }

    /** Simulate consumer overload / back-pressure — pause fetches (no FetchRequest). */
    @PostMapping("/consumer/{listenerId}/pause")
    public ResponseEntity<String> pause(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        c.pause();
        return ResponseEntity.ok("paused " + listenerId);
    }

    @PostMapping("/consumer/{listenerId}/resume")
    public ResponseEntity<String> resume(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        c.resume();
        return ResponseEntity.ok("resumed " + listenerId);
    }

    /** Simulate consumer crash — stop container (triggers rebalance / revoke). */
    @PostMapping("/consumer/{listenerId}/stop")
    public ResponseEntity<String> stop(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        c.stop();
        return ResponseEntity.ok("stopped " + listenerId + " — expect group rebalance");
    }

    @PostMapping("/consumer/{listenerId}/start")
    public ResponseEntity<String> start(@PathVariable String listenerId) {
        MessageListenerContainer c = registry.getListenerContainer(listenerId);
        if (c == null) {
            return ResponseEntity.notFound().build();
        }
        c.start();
        return ResponseEntity.ok("started " + listenerId);
    }

    @GetMapping("/failover/help")
    public Map<String, String> failoverHelp() {
        return Map.of(
                "brokerFail", "./scripts/failover-demo.sh broker-kill kafka-1",
                "brokerRecover", "./scripts/failover-demo.sh broker-recover kafka-1",
                "controllerStatus", "./scripts/failover-demo.sh controller-status",
                "preferredLeader", "./scripts/failover-demo.sh preferred-leader-election",
                "producerFail", "Stop all brokers or set acks=all with min.ISR unmet",
                "leaderFail", "Kill partition leader broker — clients refresh metadata via NotLeaderOrFollower",
                "duplicateConsume", "Stop consumer mid-batch without commit — restart replays uncommitted offsets"
        );
    }
}
