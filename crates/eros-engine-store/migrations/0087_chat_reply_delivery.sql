ALTER TABLE engine.chat_turn_queue
    ADD COLUMN reply_delivery jsonb,
    ADD COLUMN reply_due_at timestamptz,
    ADD CONSTRAINT chat_turn_queue_reply_delivery_due_check
        CHECK ((reply_delivery IS NULL) = (reply_due_at IS NULL));
