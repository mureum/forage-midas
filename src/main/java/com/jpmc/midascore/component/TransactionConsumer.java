package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;
    private final String incentivesUrl;

    public TransactionConsumer(DatabaseConduit databaseConduit, RestTemplate restTemplate, @Value("${incentives.api.url:http://localhost:8080/incentive}") String incentivesUrl) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
        this.incentivesUrl = incentivesUrl;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void receive(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        UserRecord sender = databaseConduit.findUser(senderId);
        UserRecord recipient = databaseConduit.findUser(recipientId);

        if (sender == null) {
            logger.info("Discarding transaction - sender {} not found", senderId);
            return;
        }
        if (recipient == null) {
            logger.info("Discarding transaction - recipient {} not found", recipientId);
            return;
        }
        if (sender.getBalance() < amount) {
            logger.info("Discarding transaction - insufficient funds sender {} has {} needed {}", sender.getName(), sender.getBalance(), amount);
            return;
        }

        // adjust balances (sender loses amount; recipient gains amount)
        sender.setBalance(sender.getBalance() - amount);

        // call incentives API
        float incentiveAmount = 0f;
        try {
            HttpEntity<Transaction> request = new HttpEntity<>(transaction);
            ResponseEntity<Incentive> resp = restTemplate.postForEntity(incentivesUrl, request, Incentive.class);
            if (resp != null && resp.getBody() != null) {
                incentiveAmount = resp.getBody().getAmount();
            }
        } catch (Exception e) {
            logger.warn("Incentives API call failed, proceeding with zero incentive", e);
            incentiveAmount = 0f;
        }

        // recipient gains amount + incentive (incentive is not deducted from sender)
        recipient.setBalance(recipient.getBalance() + amount + incentiveAmount);

        // persist updated users and transaction record (including incentive)
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        TransactionRecord tx = new TransactionRecord(sender, recipient, amount, incentiveAmount);
        databaseConduit.saveTransaction(tx);

        logger.info("Processed transaction {} -> {} amount {} incentive {}. New balances: {}={}, {}={}", sender.getId(), recipient.getId(), amount,
                incentiveAmount, sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}
