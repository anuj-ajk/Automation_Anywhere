"""E-mail notifications (replaces SendEmailNotification / ErrorHandler e-mails)."""
from __future__ import annotations

import smtplib
from email.message import EmailMessage

from .config import NotifyConfig


class Notifier:
    def __init__(self, cfg: NotifyConfig, log):
        self.cfg = cfg
        self.log = log

    def send(self, subject: str, body: str) -> None:
        if not self.cfg.enabled or not self.cfg.to_addresses:
            self.log.debug("E-mail '%s' not sent (notifications disabled)", subject)
            return
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.cfg.from_address
        msg["To"] = ", ".join(self.cfg.to_addresses)
        if self.cfg.cc_addresses:
            msg["Cc"] = ", ".join(self.cfg.cc_addresses)
        msg.set_content(body)
        try:
            with smtplib.SMTP(self.cfg.smtp_host, self.cfg.smtp_port, timeout=60) as smtp:
                if self.cfg.use_tls:
                    smtp.starttls()
                # No login: mails go through the SMTP relay without credentials.
                smtp.send_message(msg)
            self.log.info("E-mail '%s' sent to %d recipient(s)", subject,
                          len(self.cfg.to_addresses) + len(self.cfg.cc_addresses or []))
        except Exception as exc:  # a failed e-mail must never fail the batch
            self.log.error("Could not send notification '%s': %s", subject, exc)
