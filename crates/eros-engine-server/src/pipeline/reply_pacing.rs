// SPDX-License-Identifier: AGPL-3.0-only

use std::{collections::HashMap, time::Duration};

use eros_engine_store::chat::{AssistantInsert, ChatMessage};
use eros_engine_store::chat_queue::{
    ChatQueueRepo, NextBubble, ReplyDeliveryClaim, ScheduledBubble, REPLY_DELIVERY_MODEL_KEY,
};
use sqlx::PgPool;
use tokio::sync::oneshot;
use tokio::time::Instant;
use ulid::Ulid;

use super::stream::{ulid_string, FrameActionType, ProtocolFrame, StreamErrorCode};

pub(crate) const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const MAX_REPLY_BUBBLES: i64 = 3;
pub(super) const INDEX_METADATA_KEY: &str = "reply_bubble_index";
pub(super) const REPLY_PROMPT: &str = "\n\n像平常发消息一样，可以发一条，也可以在自然停顿处分为两到三条气泡。同一句话也可以跨气泡接着说。用一个空行分隔气泡。很短的回复可以只发一条。整轮遵守上面的语气和长度要求。";
pub(super) const FILTER_PROMPT: &str =
    "\n\n如果原回复用空行分隔了聊天气泡，尽量保留有意义的分隔，继续用空行分隔气泡。";

pub(crate) struct ReplyPacing {
    pub(crate) claim: ReplyDeliveryClaim,
    received_before_start: Duration,
    started_at: Instant,
    ready: oneshot::Sender<()>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DriveTimeout {
    Generation,
    Delivery,
}

impl DriveTimeout {
    pub(crate) fn message(&self) -> &'static str {
        match self {
            Self::Generation => "generation timeout",
            Self::Delivery => "reply delivery timeout",
        }
    }
}

impl ReplyPacing {
    pub(crate) fn new(
        claim: ReplyDeliveryClaim,
        received_at: chrono::DateTime<chrono::Utc>,
        ready: oneshot::Sender<()>,
    ) -> Self {
        Self {
            claim,
            received_before_start: (chrono::Utc::now() - received_at)
                .to_std()
                .unwrap_or_default(),
            started_at: Instant::now(),
            ready,
        }
    }

    pub(super) async fn stage(
        self,
        repo: &ChatQueueRepo<'_>,
        row: AssistantInsert,
        model: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let elapsed = self.received_before_start + self.started_at.elapsed();
        let bubbles = schedule_bubbles(row, model, elapsed);
        repo.stage_reply(self.claim, &bubbles).await?;
        let _ = self.ready.send(());
        Ok(())
    }
}

pub(crate) async fn with_deadline<T>(
    generation_timeout: Duration,
    drive: impl AsyncFnOnce(oneshot::Sender<()>) -> T,
) -> Result<T, DriveTimeout> {
    let (ready, mut delivery) = oneshot::channel();
    let drive = drive(ready);
    tokio::pin!(drive);
    let generated = tokio::time::timeout(generation_timeout, async {
        tokio::select! {
            biased;
            result = &mut drive => Some(result),
            signal = &mut delivery => {
                if signal.is_ok() { None } else { Some((&mut drive).await) }
            }
        }
    })
    .await
    .map_err(|_| DriveTimeout::Generation)?;
    match generated {
        Some(result) => Ok(result),
        None => tokio::time::timeout(DELIVERY_TIMEOUT, drive)
            .await
            .map_err(|_| DriveTimeout::Delivery),
    }
}

pub(super) fn buffer_frame(buffer: &mut Vec<ProtocolFrame>, frame: ProtocolFrame) {
    if let (
        Some(ProtocolFrame::Delta {
            message_id,
            content,
        }),
        ProtocolFrame::Delta {
            message_id: next_id,
            content: next_content,
        },
    ) = (buffer.last_mut(), &frame)
    {
        if message_id == next_id {
            content.push_str(next_content);
            return;
        }
    }
    buffer.push(frame);
}

pub(super) fn deliver_reply(
    pool: &PgPool,
    pacing: ReplyPacing,
    row: Option<AssistantInsert>,
    frames: Vec<ProtocolFrame>,
) -> impl futures_util::Stream<Item = ProtocolFrame> + Send + '_ {
    async_stream::stream! {
        let Some(row) = row else {
            for frame in frames {
                if !matches!(&frame, ProtocolFrame::Delta { content, .. } if content.trim().is_empty()) {
                    yield frame;
                }
            }
            return;
        };

        let original_id = ulid_string(row.id.into());
        let mut model = None;
        let mut usage = None;
        let mut earlier_frames = Vec::new();
        for frame in frames {
            match frame {
                ProtocolFrame::Meta { message_id, model: display, .. } if message_id == original_id => {
                    model = display;
                }
                ProtocolFrame::Delta { message_id, .. } if message_id == original_id => {}
                ProtocolFrame::Done { message_id, usage: original_usage, .. } if message_id == original_id => {
                    usage = original_usage;
                }
                frame => earlier_frames.push(frame),
            }
        }

        let repo = ChatQueueRepo { pool };
        let claim = pacing.claim;
        let original_uuid = row.id;
        if let Err(e) = pacing.stage(&repo, row, model.as_deref()).await {
            yield delivery_error(claim, e);
            return;
        }
        for frame in earlier_frames {
            yield frame;
        }
        loop {
            let row = match next_bubble(&repo, claim).await {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(e) => {
                    yield delivery_error(claim, e);
                    return;
                }
            };
            let message_id = ulid_string(row.id.into());
            yield ProtocolFrame::Meta {
                message_id: message_id.clone(),
                action_type: FrameActionType::Reply,
                model: model.clone(),
                continues_from: row.continues_from_message_id.map(Ulid::from).map(ulid_string),
                speaker_session_id: None,
            };
            yield ProtocolFrame::Delta {
                message_id: message_id.clone(),
                content: row.content,
            };
            yield ProtocolFrame::Done {
                message_id,
                truncated: false,
                usage: if row.id == original_uuid { usage.take() } else { None },
                generation_id: row.generation_id,
                ghost_fallback: false,
            };
            if row.id == original_uuid {
                break;
            }
        }
    }
}

fn delivery_error(claim: ReplyDeliveryClaim, error: sqlx::Error) -> ProtocolFrame {
    tracing::warn!(user_message_id = %claim.user_message_id, %error, "reply delivery failed");
    ProtocolFrame::Error {
        code: StreamErrorCode::Internal,
        retryable: true,
        message: "reply delivery failed".into(),
        user_message: "回复发送暂时中断，请稍后重试".into(),
        upstream_status: None,
        provider_code: None,
    }
}

pub(super) async fn next_bubble(
    repo: &ChatQueueRepo<'_>,
    claim: ReplyDeliveryClaim,
) -> Result<Option<AssistantInsert>, sqlx::Error> {
    loop {
        match repo.publish_next_bubble(claim).await? {
            NextBubble::Complete => return Ok(None),
            NextBubble::Wait(delay) => tokio::time::sleep(delay).await,
            NextBubble::Published(row) => return Ok(Some(*row)),
        }
    }
}

fn split_bubbles(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut end = 0;
    for line in text.split_inclusive('\n') {
        let next = end + line.len();
        if parts.len() < MAX_REPLY_BUBBLES as usize - 1 && line.trim().is_empty() {
            let part = text[start..end].trim();
            if !part.is_empty() {
                parts.push(part);
            }
            start = next;
        }
        end = next;
    }
    let tail = text[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

/// Model-facing history keeps one assistant message per paced reply. Place it
/// at the last visible bubble so interleaved user messages keep their order.
pub(super) fn logical_history_window(history: Vec<ChatMessage>, limit: i64) -> Vec<ChatMessage> {
    let mut latest = HashMap::new();
    let mut joined = Vec::<Option<ChatMessage>>::with_capacity(history.len());
    for mut message in history {
        let paced = message.role == "assistant"
            && message
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.get(INDEX_METADATA_KEY).is_some());
        if let Some(turn_id) = message.user_message_id.filter(|_| paced) {
            if let Some(index) = latest.insert(turn_id, joined.len()) {
                if let Some(mut previous) = joined[index].take() {
                    previous.content.push_str("\n\n");
                    previous.content.push_str(&message.content);
                    message.content = previous.content;
                }
            }
        }
        joined.push(Some(message));
    }
    let mut messages: Vec<_> = joined.into_iter().flatten().collect();
    messages.drain(..messages.len().saturating_sub(limit as usize));
    messages
}

fn typing_delay(content: &str, first_elapsed: Option<Duration>) -> Duration {
    let chars = content.chars().filter(|c| !c.is_whitespace()).count() as u64;
    let cap = if first_elapsed.is_some() { 2200 } else { 1600 };
    let target = Duration::from_millis((350 + 75 * chars).clamp(450, cap));
    target.saturating_sub(first_elapsed.unwrap_or_default())
}

fn schedule_bubbles(
    original: AssistantInsert,
    model: Option<&str>,
    elapsed: Duration,
) -> Vec<ScheduledBubble> {
    let parts = split_bubbles(&original.content);
    parts
        .iter()
        .enumerate()
        .map(|(index, content)| {
            let last = index + 1 == parts.len();
            let mut row = original.clone();
            row.content = (*content).to_owned();
            if !last {
                row.id = Ulid::new().into();
                row.generation_id = None;
                row.filter_audit = None;
                row.llm_attempts = None;
                row.gateway_errors = None;
            }
            if index != 0 {
                row.continues_from_message_id = None;
            }
            let metadata = row.metadata.get_or_insert_with(|| serde_json::json!({}));
            metadata[REPLY_DELIVERY_MODEL_KEY] = serde_json::json!(model);
            if parts.len() > 1 {
                metadata[INDEX_METADATA_KEY] = serde_json::json!(index);
            }
            if !last {
                if let Some(metadata) = metadata.as_object_mut() {
                    metadata.remove("filter_attempts");
                    metadata.remove("filter_outcome");
                    metadata.remove("f_client_msg_id");
                }
            }
            ScheduledBubble {
                row,
                delay_ms: typing_delay(content, (index == 0).then_some(elapsed)).as_millis() as u64,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_only_at_blank_lines_and_keeps_overflow() {
        assert_eq!(split_bubbles("一句话。"), ["一句话。"]);
        assert_eq!(
            split_bubbles("  \r\n\r\n我想说，\r\n \r\n其实\r\n\r\n我也想你。"),
            ["我想说，", "其实", "我也想你。"]
        );
        assert_eq!(
            split_bubbles("一\n二\n\n三\n\n四\n\n五"),
            ["一\n二", "三", "四\n\n五"]
        );
        assert!(split_bubbles(" \r\n\n").is_empty());
    }

    #[test]
    fn counts_text_and_credits_elapsed_only_on_the_first_bubble() {
        let ms = Duration::from_millis;
        assert_eq!(typing_delay("好", Some(ms(60))), ms(390));
        assert_eq!(typing_delay("好  吧", Some(ms(800))), Duration::ZERO);
        assert_eq!(typing_delay("好  吧", None), ms(500));
        assert_eq!(typing_delay("你好呀", None), ms(575));
        assert_eq!(
            typing_delay(&"好".repeat(100), Some(Duration::ZERO)),
            ms(2200)
        );
        assert_eq!(typing_delay(&"好".repeat(100), None), ms(1600));
    }

    #[test]
    fn model_history_counts_a_paced_reply_once_even_with_interleaved_input() {
        let session = uuid::Uuid::new_v4();
        let turn = uuid::Uuid::new_v4();
        let row = |id, role: &str, content: &str, turn_id, metadata| {
            serde_json::from_value(serde_json::json!({
                "id": id, "session_id": session, "role": role, "content": content,
                "sent_at": "2026-11-21T00:00:00Z", "user_message_id": turn_id,
                "metadata": metadata,
            }))
            .unwrap()
        };
        let old_user = row(turn, "user", "先前的话", None, None);
        let first = row(
            uuid::Uuid::new_v4(),
            "assistant",
            "我想说",
            Some(turn),
            Some(serde_json::json!({ (INDEX_METADATA_KEY): 0 })),
        );
        let new_user = row(uuid::Uuid::new_v4(), "user", "刚收到", None, None);
        let last = row(
            uuid::Uuid::new_v4(),
            "assistant",
            "其实我也想你",
            Some(turn),
            Some(serde_json::json!({ (INDEX_METADATA_KEY): 1 })),
        );
        let rows = logical_history_window(vec![old_user, first, new_user, last], 2);
        assert_eq!(
            rows.iter()
                .map(|row| row.content.as_str())
                .collect::<Vec<_>>(),
            ["刚收到", "我想说\n\n其实我也想你"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_wait_uses_its_own_deadline() {
        let ms = Duration::from_millis;
        let start = Instant::now();
        assert_eq!(
            with_deadline(ms(100), async |ready| {
                tokio::time::sleep(ms(90)).await;
                ready.send(()).unwrap();
                tokio::time::sleep(ms(350)).await;
                42
            })
            .await,
            Ok(42)
        );
        assert_eq!(start.elapsed(), ms(440));
        assert_eq!(
            with_deadline(ms(100), async |_ready| {
                tokio::time::sleep(ms(150)).await;
            })
            .await,
            Err(DriveTimeout::Generation)
        );
        assert_eq!(
            with_deadline(ms(100), async |ready| {
                ready.send(()).unwrap();
                tokio::time::sleep(DELIVERY_TIMEOUT + ms(1)).await;
            })
            .await,
            Err(DriveTimeout::Delivery)
        );
    }
}
