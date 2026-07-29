package com.ecommerce.order.replayservice.controller;

import com.ecommerce.order.kafka.dlq.DeadLetterInspectionService;
import com.ecommerce.order.kafka.dlq.DeadLetterReprocessService;
import com.ecommerce.order.kafka.dlq.DlqReprocessException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * REST API to inspect and reprocess dead-letter messages.
 *
 * <pre>
 * POST /api/v1/dlq/reprocess
 * POST /api/v1/dlq/reprocess/range
 * GET  /api/v1/dlq/messages?partition=0&amp;offset=12
 * GET  /api/v1/dlq/messages/scan?partition=0&amp;fromOffset=0&amp;max=20
 * </pre>
 */
@RestController
@RequestMapping("/api/v1/dlq")
@Validated
public class DeadLetterReprocessController {

    private final DeadLetterReprocessService reprocessService;
    private final DeadLetterInspectionService inspectionService;

    public DeadLetterReprocessController(
            DeadLetterReprocessService reprocessService,
            DeadLetterInspectionService inspectionService
    ) {
        this.reprocessService = reprocessService;
        this.inspectionService = inspectionService;
    }

    @PostMapping("/reprocess")
    public DeadLetterReprocessService.ReprocessResult reprocessOne(
            @Valid @RequestBody ReprocessOneRequest request
    ) {
        return reprocessService.reprocessByOffset(
                request.partition(),
                request.offset(),
                request.force()
        );
    }

    @PostMapping("/reprocess/range")
    public List<DeadLetterReprocessService.ReprocessResult> reprocessRange(
            @Valid @RequestBody ReprocessRangeRequest request
    ) {
        return reprocessService.reprocessRange(
                request.partition(),
                request.fromOffset(),
                request.toOffset(),
                request.force()
        );
    }

    @GetMapping("/messages")
    public ResponseEntity<DeadLetterInspectionService.DlqMessageView> peek(
            @RequestParam @Min(0) int partition,
            @RequestParam @Min(0) long offset
    ) {
        return inspectionService.peek(partition, offset)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/messages/scan")
    public List<DeadLetterInspectionService.DlqMessageView> scan(
            @RequestParam @Min(0) int partition,
            @RequestParam @Min(0) long fromOffset,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int max
    ) {
        return inspectionService.peekFrom(partition, fromOffset, max);
    }

    @ExceptionHandler(DlqReprocessException.class)
    public ResponseEntity<Map<String, String>> handleDlq(DlqReprocessException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    public record ReprocessOneRequest(
            @NotNull @Min(0) Integer partition,
            @NotNull @Min(0) Long offset,
            boolean force
    ) {
    }

    public record ReprocessRangeRequest(
            @NotNull @Min(0) Integer partition,
            @NotNull @Min(0) Long fromOffset,
            @NotNull @Min(0) Long toOffset,
            boolean force
    ) {
    }
}
