package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final DatabaseConduit databaseConduit;

    public TransactionConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
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

        // adjust balances
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);

        // persist updated users and transaction record
        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        TransactionRecord tx = new TransactionRecord(sender, recipient, amount);
        databaseConduit.saveTransaction(tx);

        logger.info("Processed transaction {} -> {} amount {}. New balances: {}={}, {}={}", sender.getId(), recipient.getId(), amount,
                sender.getName(), sender.getBalance(), recipient.getName(), recipient.getBalance());
    }
}
