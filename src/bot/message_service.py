"""Утилиты отправки сообщений в Telegram с безопасным разбиением."""

from __future__ import annotations

from aiogram.types import Message

from src.core.logging_utils import bind_context


class TelegramMessageSender:
    """Отправляет сообщения в Telegram и делит длинные тексты на части.

    У Telegram есть ограничение на размер сообщения, поэтому большие ответы
    (например, табличные) разбиваются перед отправкой.
    """

    def __init__(self, max_message_len: int = 4000) -> None:
        self._max_message_len = max_message_len

    def split_message(self, text: str) -> list[str]:
        """Разбивает длинный текст на части, сохраняя читаемость по строкам.

        Логика сохранена 1 в 1: сначала разбиение по строкам, если строка
        слишком длинная — разбиение по пробелам с жёсткой границей как fallback.
        """
        if len(text) <= self._max_message_len:
            return [text]

        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for line in text.splitlines():
            line_with_nl = f"{line}\n"
            if len(line_with_nl) > self._max_message_len:
                if current:
                    chunks.append("".join(current).rstrip())
                    current = []
                    current_len = 0

                rest = line
                while len(rest) > self._max_message_len:
                    split_at = rest.rfind(" ", 0, self._max_message_len)
                    if split_at <= 0:
                        split_at = self._max_message_len
                    chunks.append(rest[:split_at].rstrip())
                    rest = rest[split_at:].lstrip()
                if rest:
                    current = [rest + "\n"]
                    current_len = len(current[0])
                continue

            if current_len + len(line_with_nl) > self._max_message_len:
                chunks.append("".join(current).rstrip())
                current = [line_with_nl]
                current_len = len(line_with_nl)
            else:
                current.append(line_with_nl)
                current_len += len(line_with_nl)

        if current:
            chunks.append("".join(current).rstrip())

        return chunks

    async def send_long_message(self, message: Message, text: str, parse_mode: str | None = None) -> None:
        """Отправляет текст одним или несколькими сообщениями с логированием."""
        log = bind_context(task_name="tg_send", endpoint="message")
        parts = self.split_message(text)
        effective_parse_mode = parse_mode or "HTML"
        log.info("Sending message total_len={} chunks={}", len(text), len(parts))

        for idx, part in enumerate(parts, start=1):
            try:
                await message.answer(part, parse_mode=effective_parse_mode)
                log.info("Chunk sent idx={} size={}", idx, len(part))
            except Exception as exc:
                log.exception("Failed to send chunk idx={} size={} error={}", idx, len(part), exc)
                raise
