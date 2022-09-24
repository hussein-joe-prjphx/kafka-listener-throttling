/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import com.common.Bar2;
import com.common.Foo2;
import org.apache.kafka.common.utils.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gary Russell
 * @since 5.1
 */
@Component
public class KafkaProcessors {
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	private AtomicInteger totalProcessedMessages = new AtomicInteger(0);

	KafkaProcessors(@Autowired KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
		this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
	}

	private final TaskExecutor exec = new SimpleAsyncTaskExecutor();
	private final ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);


	@KafkaListener(id = "batchProcessor", topics = "batch_processor_topic", properties = {"max.poll.records=5"})
	public void batchProcessingKafkaListener(Foo2 foo) {
		System.out.println("Received a batch message: " + foo);
		this.totalProcessedMessages.incrementAndGet();
		this.pauseKafkaIfRequired();
		//terminateMessage();
	}

	@KafkaListener(id = "realtimeProcessor", topics = "realtime_topic", properties = {"max.poll.records=5"})
	public void realtimeKafkaListener(Bar2 bar) {
		System.out.println("Received a realtime: " + bar);
	}

	private void pauseKafkaIfRequired() {
		MessageListenerContainer batchProcessor = this.kafkaListenerEndpointRegistry.getListenerContainer("batchProcessor");
		if (this.totalProcessedMessages.get() > 5 && !batchProcessor.isPauseRequested()) {
			int resumeAfterMs = 60 * 1000;
			System.out.println("The batch processor is going in sleep mode and going to resume after 60 seconds starting at " + LocalDateTime.now());
			batchProcessor.pause();
			Scheduler.SYSTEM.schedule(this.scheduledExecutorService, this::resumeProcessor, resumeAfterMs);
		}
	}

	private Integer resumeProcessor() {
		System.out.println("The batch processor is resuming at " + LocalDateTime.now());
		MessageListenerContainer batchProcessor = this.kafkaListenerEndpointRegistry.getListenerContainer("batchProcessor");
		if (batchProcessor.isPauseRequested()) {
			batchProcessor.resume();
			this.totalProcessedMessages.set(0);
		}
		return 0;
	}

}
