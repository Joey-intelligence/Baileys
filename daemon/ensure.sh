#!/bin/bash
# Reinstall and start the baileys-wa systemd unit (the unit lives in
# ephemeral /etc and vanishes on VM replacement; this project dir persists).
set -e
cp "$(dirname "$0")/baileys-wa.service" /etc/systemd/system/baileys-wa.service
systemctl daemon-reload
systemctl enable baileys-wa.service >/dev/null 2>&1 || true
systemctl start baileys-wa.service
systemctl status baileys-wa.service --no-pager | head -8
