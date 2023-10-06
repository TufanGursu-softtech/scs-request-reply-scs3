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
public class Pong {
    @Builder.Default
    private String message = "pong";
    private Long duration;
    private String requestCid;
    private String responseCid;
}

