package com.pubsub.manager.controller;


import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.AcknowledgeablePubsubMessage;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collection;

@RestController
public class PubSubManagerController {

    private static final Log LOGGER = LogFactory.getLog(PubSubManagerController.class);

    private final PubSubTemplate pubSubTemplate;

    private final PubSubAdmin pubSubAdmin;

    private final ArrayList<Subscriber> allSubscribers;

    @Autowired
    public PubSubManagerController(PubSubTemplate pubSubTemplate,
                                   PubSubAdmin pubSubAdmin) {
        this.pubSubTemplate = pubSubTemplate;
        this.pubSubAdmin    = pubSubAdmin;
        this.allSubscribers = new ArrayList<>();
    }



    @GetMapping("/postMessage")
    public ResponseEntity<String> publish(@RequestParam("topicName") String topicName,
                                          @RequestParam("message") String message,
                                          @RequestParam("count") int messageCount) {
        for (int i = 0; i < messageCount; i++) {
            this.pubSubTemplate.publish(topicName, message);
        }

        return new ResponseEntity<>("Mensaje publicado en GCP.", HttpStatus.OK);
    }


    @GetMapping("/pull")
    public ResponseEntity<String> pull(@RequestParam("subscription1") String subscriptionName) {

        Collection<AcknowledgeablePubsubMessage> messages = this.pubSubTemplate.pull(subscriptionName, 10, true);

        if (messages.isEmpty()) {
            return new ResponseEntity<>("no hay mensajes disponibles", HttpStatus.OK);
        }


        try {
            ListenableFuture<Void> ackFuture = this.pubSubTemplate.ack(messages);
            ackFuture.get();

            messages.forEach(acknowledgeablePubsubMessage -> {
                PubsubMessage pubsubMessage = acknowledgeablePubsubMessage.getPubsubMessage();

                System.out.println("Viendo los mensajes obtenidos");
            });
        }
        catch (Exception ex) {
            LOGGER.warn("error al hacer ACK de un mensaje", ex);

        }

        return new ResponseEntity<>("terminamos de recuperar mensajes desde gcp", HttpStatus.OK);
    }


    @GetMapping("/subscribe")
    public ResponseEntity<String> subscribe(@RequestParam("subscription") String subscriptionName) {
        Subscriber subscriber = this.pubSubTemplate.subscribe(subscriptionName, message -> {
            LOGGER.info("Message received from " + subscriptionName + " subscription: "
                    + message.getPubsubMessage().getData().toStringUtf8());
            message.ack();
        });

        this.allSubscribers.add(subscriber);
        return new ResponseEntity<>("suscrito ", HttpStatus.OK);
    }

}
