package com.example;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Ping {
    @Builder.Default
    private String message = "ping";
    private String cid;
    private Long currentTimeMillis;
}
