---
name: discord-openai-realtime-voice-setup
description: Use when setting up, validating, or troubleshooting Hermes Agent as a Discord voice-channel bot using OpenAI Realtime speech-to-speech mode and Hermes subagent handoff.
version: 1.0.0
author: Hermes Agent
license: MIT
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [discord, voice, openai-realtime, gateway, setup, diagnostics]
    related_skills: [hermes-agent, systematic-debugging]
---

# Discord OpenAI Realtime Voice Setup

## Overview

Use this skill to help another Hermes agent set up or debug Hermes Agent running as a real-time Discord voice assistant. The target production path is the Discord slash command:

```text
/voice realtime
```

That path joins the user's current Discord voice channel and uses OpenAI Realtime for low-latency speech-to-speech conversation. Longer tasks should be handed off to normal Hermes subagents so filesystem, tools, memory, approvals, and durable text-channel reporting stay in the regular Hermes execution path.

The companion user-facing guide lives at:

```text
website/docs/guides/hermes-discord-openai-realtime-voice-setup.md
```

## When to Use

Use this skill when the user asks for any of these:

- Set up Hermes with Discord voice channels.
- Enable `/voice realtime`.
- Configure OpenAI Realtime for Hermes voice.
- Diagnose Discord voice-channel audio failures.
- Explain prerequisites for Discord Realtime voice.
- Create or audit setup docs for another Hermes agent.
- Distinguish Hermes-native voice mode from OpenAI Realtime voice mode.

Do not use this skill for generic Discord text-only bot setup unless the request also mentions voice, Realtime, OpenAI speech-to-speech, STT/TTS, or voice channels.

## Architecture

There are two Discord voice engines:

| Engine | Command | Pipeline |
|---|---|---|
| Hermes-native voice | `/voice join` | Discord audio -> STT -> Hermes agent/tools/memory -> TTS -> Discord audio |
| OpenAI Realtime voice | `/voice realtime` | Discord audio -> OpenAI Realtime WebSocket -> Discord audio, with safe Hermes subagent handoff |

Explain this clearly. Realtime voice is not just a provider toggle for normal Hermes text inference. It is a separate voice-engine path.

## Required Inputs

Collect or verify these without asking if they are discoverable locally:

1. Hermes Agent install or source checkout.
2. Discord bot token in `~/.hermes/.env` as `DISCORD_BOT_TOKEN`.
3. Discord allowlist in `DISCORD_ALLOWED_USERS` or `DISCORD_ALLOWED_ROLES`.
4. OpenAI Platform key in one of:
   - `OPENAI_REALTIME_API_KEY`
   - `VOICE_TOOLS_OPENAI_KEY`
   - `OPENAI_API_KEY`
5. Memory provider posture:
   - Joe-style/personal setup: Honcho enabled with `recallMode: "hybrid"`.
   - Hosted Honcho: `HONCHO_API_KEY` present in `~/.hermes/.env`.
   - Self-hosted Honcho: `baseUrl` present in `$HERMES_HOME/honcho.json`, `~/.hermes/honcho.json`, or `~/.honcho/config.json`.
   - Single-operator Discord/Telegram/CLI: `pinUserPeer: true` or `userPeerAliases` maps the operator's Discord user ID to the main peer.
6. Discord bot has voice permissions in the target server/channel:
   - View Channels
   - Send Messages
   - Read Message History
   - Connect
   - Speak
   - Use Voice Activity
7. Discord Developer Portal privileged intents enabled:
   - Presence Intent
   - Server Members Intent
   - Message Content Intent

## Setup Procedure

### 1. Install system dependencies

macOS:

```bash
brew install ffmpeg opus portaudio espeak-ng
```

Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y ffmpeg libopus0 portaudio19-dev espeak-ng
```

### 2. Install Python dependencies

Package install:

```bash
python -m pip install -U "hermes-agent[messaging,voice]"
python -m pip install -U "discord.py[voice]" "PyNaCl>=1.5.0" davey websockets
```

Source checkout:

```bash
cd /path/to/hermes-agent
python -m venv venv
source venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[messaging,voice]"
python -m pip install -U "discord.py[voice]" "PyNaCl>=1.5.0" davey websockets
```

Optional Hermes-native fallback providers:

```bash
python -m pip install -U faster-whisper edge-tts
```

### 3. Create and configure Discord bot

Guide the user through the Discord Developer Portal:

1. Create application.
2. Open Bot page.
3. Copy/reset bot token.
4. Enable Presence, Server Members, and Message Content privileged intents.
5. Invite with scopes `bot` and `applications.commands`.
6. Include voice permissions.

Manual invite URL shape:

```text
https://discord.com/oauth2/authorize?client_id=YOUR_APP_ID&scope=bot+applications.commands&permissions=274878286912
```

### 4. Configure `~/.hermes/.env`

```bash
DISCORD_BOT_TOKEN=your-discord-bot-token
DISCORD_ALLOWED_USERS=your-discord-user-id

# Prefer a dedicated key for Realtime voice.
OPENAI_REALTIME_API_KEY=sk-...

# Optional fallback key names Hermes also accepts.
# VOICE_TOOLS_OPENAI_KEY=sk-...
# OPENAI_API_KEY=sk-...

OPENAI_REALTIME_MODEL=gpt-realtime
OPENAI_REALTIME_VOICE=alloy
OPENAI_REALTIME_INSTRUCTIONS=You are Hermes on Discord voice. Respond in English only. Keep spoken replies concise. For long research, coding, browsing, debugging, file edits, or multi-step work, call the Hermes subagent handoff tool.

HERMES_REALTIME_SILENCE_SECONDS=0.65
HERMES_REALTIME_MIN_SPEECH_SECONDS=0.45
HERMES_REALTIME_MIN_RMS=180
```

Never ask the user to paste secrets into chat if local file access is available. Inspect config/key presence without revealing full values.

### 5. Configure `~/.hermes/config.yaml`

Use conservative fallback voice defaults:

```yaml
stt:
  provider: "local"
  local:
    model: "base"

tts:
  provider: "edge"
  edge:
    voice: "en-US-AriaNeural"

voice:
  auto_tts: false
  max_recording_seconds: 120
  silence_threshold: 200
  silence_duration: 3.0

discord:
  require_mention: true
  auto_thread: true
  reactions: true
```

Realtime mode uses OpenAI audio directly, but these settings keep `/voice join` and fallback voice messages testable.

### 5A. Configure Honcho / memory for Joe-style replication

For a personal operator setup, prefer Honcho hybrid mode so the Realtime voice sidecar can start with bounded context and normal Hermes subagents can use memory tools for durable work.

Fresh install:

```bash
python -m pip install -U honcho-ai
hermes memory setup honcho
```

Manual minimum:

```bash
echo 'HONCHO_API_KEY=***' >> ~/.hermes/.env
hermes config set memory.provider honcho
```

Minimal `honcho.json` shape for a single-operator Discord server:

```json
{
  "enabled": true,
  "workspace": "hermes",
  "peerName": "joe",
  "aiPeer": "hermes",
  "recallMode": "hybrid",
  "pinUserPeer": true,
  "sessionStrategy": "per-directory",
  "writeFrequency": "async",
  "saveMessages": true
}
```

If the bot serves multiple humans, do not pin every runtime user to the operator peer. Use `userPeerAliases` for the operator's Discord/Telegram IDs and let other users resolve to separate peers.

## Verification Procedure

Always verify layer-by-layer. Do not jump straight to `/voice realtime`.

### 1. Text bot verification

Start the gateway:

```bash
hermes gateway
```

Then test in Discord:

```text
/whoami
```

Or DM/mention the bot with a simple prompt.

### 2. Discord voice dependency doctor

From a Hermes Agent checkout:

```bash
python scripts/discord-voice-doctor.py
```

The doctor checks Python voice packages, Opus, ffmpeg, Discord token, authorization posture, bot login, guild membership, and voice permissions.

### 3. OpenAI Realtime smoke tests

If available in the environment:

```bash
python ~/.hermes/skills/devops/hermes-operations/scripts/openai_realtime_text_smoke.py
python ~/.hermes/skills/devops/hermes-operations/scripts/openai_realtime_audio_smoke.py
```

Expected shape:

```text
connecting model=gpt-realtime key_source=OPENAI_REALTIME_API_KEY
assistant_text: realtime text ok

audio_bytes: <nonzero>
transcript: realtime audio ok
wav: /tmp/openai_realtime_smoke.wav
```

### 4. Hermes-native Discord voice test

In Discord, join a voice channel and run from a text channel the bot can see:

```text
/voice join
/voice status
/voice leave
```

This proves Discord voice transport before testing Realtime.

### 5. Realtime Discord voice test

Join a voice channel and run:

```text
/voice realtime
/voice status
```

Speak a short instruction. Confirm spoken response. Then test a handoff-type prompt, such as:

```text
Research this briefly and post the result in the channel.
```

A good result is concise spoken acknowledgement plus durable text-channel output from the Hermes subagent.

## PASS / PARTIAL / BLOCKED Reporting

Report status precisely:

### PASS

Use PASS only when:

- text bot works;
- slash command routing works;
- voice doctor passes or warnings are non-blocking and understood;
- OpenAI Realtime key smoke works;
- `/voice join` works;
- `/voice realtime` joins the VC and speaks back;
- subagent handoff posts durable result in the linked text channel.

### PARTIAL

Use PARTIAL when:

- OpenAI Realtime smoke works but Discord voice channel transport is unverified;
- `/voice realtime` speaks but subagent completion relay is not verified;
- Hermes-native voice works but Realtime key/model access is not verified;
- Discord slash command cache is stale but local implementation is present.

### BLOCKED

Use BLOCKED when:

- no Discord bot token is configured;
- no OpenAI Realtime-capable key is configured;
- the bot lacks Connect/Speak permissions;
- voice dependencies cannot be installed;
- the machine cannot reach Discord or OpenAI;
- the available tooling cannot run commands or push commits and the user specifically asked for those operations.

## Common Pitfalls

1. **Text gateway works, voice still broken.** Text does not prove Opus, PyNaCl, DAVE, ffmpeg, voice permissions, or RTP receive/playback.

2. **OpenAI key present but unusable.** A set env var can still fail due to billing, project permissions, model entitlement, wrong key, or empty `KEY=` value.

3. **Discord command cache stale.** If `/voice realtime` is missing from the Discord slash picker, inspect command sync/registration before assuming runtime support is absent.

4. **Message Content Intent disabled.** Bot may connect but not read normal server messages.

5. **Bot role lacks channel-level voice permissions.** Server-level invite permissions can be overridden in private channels.

6. **Realtime gets too chatty.** Tighten `OPENAI_REALTIME_INSTRUCTIONS`; require English-only, concise responses, and subagent handoff for long/tool tasks.

7. **Noise triggers turns.** Increase `HERMES_REALTIME_MIN_RMS`, `HERMES_REALTIME_MIN_SPEECH_SECONDS`, or `HERMES_REALTIME_SILENCE_SECONDS`.

8. **Confusing `/voice join` and `/voice realtime`.** `/voice join` is Hermes-native STT/TTS. `/voice realtime` is OpenAI Realtime speech-to-speech.

## Operator Response Template

When reporting to Joe or another operator, keep it compact:

```text
STATUS: PASS/PARTIAL/BLOCKED

What changed:
- ...

Verified:
- ... exact commands/tests run ...

Use it:
- Join Discord VC
- Run /voice realtime in the linked text channel
- Use /voice leave to stop

Blockers / notes:
- ...
```

## Skill Vetting Note

This skill is documentation-only. It does not include executable scripts, network fetchers, credential access code, installers, or hidden tool invocations. Risk level: LOW. Still review edits before trusting future changes.
