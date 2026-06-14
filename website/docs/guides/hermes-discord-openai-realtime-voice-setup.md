---
sidebar_position: 9
title: "Hermes Discord OpenAI Realtime Voice Setup"
description: "Full setup guide for running Hermes Agent as a real-time Discord voice assistant using OpenAI Realtime."
---

# Hermes Agent + Discord + OpenAI Realtime Voice Setup

This guide explains how to set up Hermes Agent so a Discord bot can join a voice channel and run low-latency speech-to-speech conversations through OpenAI Realtime, with Hermes subagent handoff for longer tool work.

PDF version: `/guides/hermes-discord-openai-realtime-voice-setup.pdf`

Assumptions used by this guide:
- You are setting up a normal Hermes Agent checkout or installed package.
- You control a Discord server where you can invite bots.
- You have access to an OpenAI Platform API key with Realtime API access.
- You want the production Discord voice-channel command path: `/voice realtime`.

Reference URLs:
- Hermes docs: https://hermes-agent.nousresearch.com/docs
- Discord Developer Portal: https://discord.com/developers/applications
- OpenAI API keys: https://platform.openai.com/api-keys
- OpenAI Realtime docs: https://platform.openai.com/docs/guides/realtime

## Full pre-flight checklist: Joe-style Discord voice agent

Use this checklist before you call the environment reproducible. It covers the pieces Joe's working setup depends on: Discord text, Discord voice, OpenAI Realtime, Hermes subagent handoff, Honcho memory, and safe operator boundaries.

### A. Repository and runtime

- [ ] Hermes Agent is installed from a package or a source checkout.
- [ ] Python 3.11+ is available.
- [ ] The Hermes virtual environment is active when running source-checkout commands.
- [ ] `ffmpeg` is installed and available on `PATH`.
- [ ] Opus libraries are installed so Discord voice can encode/decode audio.
- [ ] Messaging and voice dependencies are installed: `hermes-agent[messaging,voice]`, `discord.py[voice]`, `PyNaCl`, `davey`, and `websockets`.
- [ ] Optional Hermes-native fallback voice packages are installed if you want `/voice join`: `faster-whisper` and `edge-tts` or another configured STT/TTS pair.
- [ ] `.env`, `config.yaml`, `honcho.json`, auth files, dashboard tokens, Discord tokens, and provider keys are not committed.

### B. Discord application and server

- [ ] A Discord application exists at https://discord.com/developers/applications.
- [ ] The application has a bot user and a current bot token.
- [ ] Privileged Gateway Intents are enabled: Presence Intent, Server Members Intent, and Message Content Intent.
- [ ] The bot is invited with scopes `bot` and `applications.commands`.
- [ ] The bot role has text permissions in the command channel: View Channels, Send Messages, Read Message History, and Attach Files.
- [ ] The bot role has voice permissions in the target voice channel: Connect, Speak, and Use Voice Activity.
- [ ] Channel-specific overwrites do not remove the bot's text or voice permissions.
- [ ] Discord Developer Mode is enabled so you can copy user, channel, guild, and role IDs.
- [ ] Hermes is locked down with `DISCORD_ALLOWED_USERS` and/or `DISCORD_ALLOWED_ROLES`. Do not run a personal operator agent open to a whole server unless that is intentional.

### C. OpenAI Realtime

- [ ] You have a normal OpenAI Platform API key from https://platform.openai.com/api-keys. Codex OAuth/login credentials are not enough for Realtime.
- [ ] The key has billing/project/model access for the Realtime model you plan to use.
- [ ] The key is stored in `~/.hermes/.env` using the preferred dedicated name `OPENAI_REALTIME_API_KEY`.
- [ ] Fallback key names are understood but not preferred: `VOICE_TOOLS_OPENAI_KEY`, then `OPENAI_API_KEY`.
- [ ] `OPENAI_REALTIME_MODEL` is set or defaults to a Realtime-capable model such as `gpt-realtime`.
- [ ] `OPENAI_REALTIME_VOICE` is set if you want a non-default Realtime voice.
- [ ] `OPENAI_REALTIME_INSTRUCTIONS` tells the spoken agent to stay concise, English-only when appropriate, and delegate long/tool work to Hermes subagents.
- [ ] Turn-detection knobs are set conservatively enough for your room: `HERMES_REALTIME_SILENCE_SECONDS`, `HERMES_REALTIME_MIN_SPEECH_SECONDS`, and `HERMES_REALTIME_MIN_RMS`.

### D. Hermes memory and Honcho

- [ ] Decide the memory provider before running live voice. Joe-style setup uses Honcho in hybrid mode so Hermes can both inject bounded context and expose memory tools.
- [ ] Install the Honcho SDK if using Honcho: `pip install honcho-ai`.
- [ ] Configure Honcho with `hermes memory setup honcho` on a fresh install. The older `hermes honcho setup` command only works after Honcho is already the active provider.
- [ ] Store `HONCHO_API_KEY` in `~/.hermes/.env` when using hosted Honcho, or set a self-hosted `baseUrl` in Honcho config.
- [ ] Confirm config resolution path: `$HERMES_HOME/honcho.json` first, then `~/.hermes/honcho.json`, then `~/.honcho/config.json`.
- [ ] For a single-user personal Discord server, set `pinUserPeer: true` or alias the operator's Discord user ID to the main peer with `userPeerAliases`.
- [ ] Set `recallMode: "hybrid"` for Joe-style behavior: context injection plus tools.
- [ ] Set a stable `peerName` for the human operator and stable `aiPeer` for the Hermes profile.
- [ ] Verify memory tools are available when expected: `honcho_profile`, `honcho_search`, `honcho_context`, `honcho_reasoning`, and `honcho_conclude`.

### E. Subagent handoff and operator boundaries

- [ ] Realtime voice is treated as a live chief-of-staff sidecar, not a raw terminal agent.
- [ ] The Realtime model has only a narrow server-side handoff tool such as `start_subagent_task` / `handoff_to_hermes`.
- [ ] Raw shell, filesystem, credential files, and unrestricted Discord admin actions are not exposed directly to the Realtime model.
- [ ] Long coding, browsing, debugging, research, build/test/deploy, and file-edit tasks go to normal Hermes workers/subagents with regular tools, memory, approvals, and text-channel audit trail.
- [ ] The bound Discord text channel receives durable completion output for handoff tasks; voice only gives compact status unless asked for detail.
- [ ] Human approval gates remain intact for high-blast-radius operations: production deploys, cron resumption, credential changes, spend, destructive file operations, and broad GitHub pushes.

### F. Verification gate

- [ ] `python scripts/discord-voice-doctor.py` passes or every warning is explicitly understood.
- [ ] OpenAI Realtime text smoke succeeds.
- [ ] OpenAI Realtime audio smoke returns nonzero audio and an expected transcript.
- [ ] `hermes gateway` starts and the bot responds in Discord text.
- [ ] `/whoami` or a basic mention/DM confirms the authorized identity.
- [ ] `/voice join`, `/voice status`, and `/voice leave` work before testing Realtime.
- [ ] `/voice realtime` joins the user's current voice channel and speaks back.
- [ ] A prompt that requires durable work triggers subagent handoff and posts the result in the linked text channel.

### Minimal Joe-style `.env` shape

```bash
DISCORD_BOT_TOKEN=your-discord-bot-token
DISCORD_ALLOWED_USERS=your-discord-user-id

OPENAI_REALTIME_API_KEY=sk-...
OPENAI_REALTIME_MODEL=gpt-realtime
OPENAI_REALTIME_VOICE=alloy
OPENAI_REALTIME_INSTRUCTIONS=You are Hermes on Discord voice. Respond in English only. Keep spoken replies concise. For coding, browsing, debugging, research, file edits, deployments, or other multi-step work, start a Hermes subagent task and post the durable result to the bound Discord text channel.

HERMES_REALTIME_SILENCE_SECONDS=0.65
HERMES_REALTIME_MIN_SPEECH_SECONDS=0.45
HERMES_REALTIME_MIN_RMS=180

HONCHO_API_KEY=...
```

### Minimal Joe-style `honcho.json` shape

For a single-operator Discord/Telegram/CLI setup, keep all of the operator's surfaces mapped to one human peer:

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

If the bot will serve multiple humans, do not use `pinUserPeer: true`; instead map only the operator's runtime IDs with `userPeerAliases` and give unknown users separate peers.

## 1. What you are building

The Realtime Discord voice path is different from normal text chat and different from Hermes-native STT/TTS voice mode.

```text
Discord voice RTP/Opus -> PCM -> OpenAI Realtime WebSocket -> Discord voice playback
                                      |
                                      +-> safe Hermes subagent handoff for long/tool tasks
```

Hermes supports two Discord voice-channel engines:

| Engine | Discord command | What it does |
|---|---|---|
| Hermes-native voice | `/voice join` | Discord audio -> STT -> normal Hermes agent/tools/memory -> TTS -> Discord audio |
| OpenAI Realtime voice | `/voice realtime` | Discord audio -> persistent OpenAI Realtime speech session -> audio reply, with safe subagent handoff for heavier Hermes work |

Use `/voice realtime` when you want the lowest-latency spoken sidecar. Use `/voice join` when you want the classic Hermes-native STT/TTS pipeline.

## 2. Prerequisites

### Accounts and access

You need:

1. A Discord account.
2. A Discord server where you have **Manage Server** permission, or an admin willing to invite the bot.
3. A Discord application and bot token from the Discord Developer Portal.
4. An OpenAI Platform API key that can use Realtime.
5. A machine that can run the Hermes gateway continuously.

### Local system requirements

Recommended:

- Python 3.11 or newer.
- `ffmpeg` for audio conversion.
- Opus codec libraries for Discord voice.
- Hermes Agent installed from package or source checkout.
- `discord.py[voice]`, PyNaCl, and DAVE/E2EE support for Discord voice receive/playback.

macOS:

```bash
brew install ffmpeg opus portaudio espeak-ng
```

Ubuntu / Debian:

```bash
sudo apt update
sudo apt install -y ffmpeg libopus0 portaudio19-dev espeak-ng
```

## 3. Install Hermes voice and messaging dependencies

If installing from PyPI, use the full or targeted extras:

```bash
python -m pip install -U "hermes-agent[messaging,voice]"
```

For the broadest install:

```bash
python -m pip install -U "hermes-agent[all]"
```

If working from a source checkout, activate the repo virtual environment first and install the project with the same extras:

```bash
cd /path/to/hermes-agent
python -m venv venv
source venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[messaging,voice]"
```

Discord voice also depends on:

```bash
python -m pip install -U "discord.py[voice]" "PyNaCl>=1.5.0" davey websockets
```

Optional speech providers for Hermes-native mode:

```bash
python -m pip install -U faster-whisper edge-tts
```

OpenAI Realtime mode does not require local Whisper/TTS for the direct speech-to-speech path, but keeping Hermes-native voice working is useful for fallback and diagnostics.

## 4. Create the Discord bot

1. Open https://discord.com/developers/applications.
2. Click **New Application**.
3. Name it, for example `Hermes Agent`.
4. Open the **Bot** page.
5. Reset/copy the bot token. Store it securely. Do not commit it.
6. Under **Privileged Gateway Intents**, enable:
   - Presence Intent
   - Server Members Intent
   - Message Content Intent
7. Save changes.

Message Content Intent is the usual footgun: without it, the bot may connect but cannot read normal text messages.

## 5. Invite the bot to your server

In the Developer Portal, use the Installation tab or construct a manual OAuth URL:

```text
https://discord.com/oauth2/authorize?client_id=YOUR_APP_ID&scope=bot+applications.commands&permissions=274878286912
```

Replace `YOUR_APP_ID` with the application ID.

For voice-channel operation, make sure the bot has at least:

- View Channels
- Send Messages
- Read Message History
- Attach Files
- Connect
- Speak
- Use Voice Activity

If you are testing in a private channel, verify the bot's role has those permissions in that channel too.

## 6. Get IDs for authorization

Turn on Discord Developer Mode:

1. Discord Settings -> Advanced -> Developer Mode -> ON.
2. Right-click your user -> Copy User ID.
3. Optional: right-click the target channel -> Copy Channel ID.
4. Optional: right-click trusted roles -> Copy Role ID.

Hermes should be locked down. At least one of `DISCORD_ALLOWED_USERS` or `DISCORD_ALLOWED_ROLES` should be set.

## 7. Get an OpenAI API key

1. Go to https://platform.openai.com/api-keys.
2. Create a new API key.
3. Store it in a password manager or secret store.
4. Confirm the key has access to the Realtime API/model you plan to use.

OpenAI Codex OAuth is not a substitute for an OpenAI Platform API key. Realtime needs a normal API key.

Hermes checks Realtime keys in this order:

1. `OPENAI_REALTIME_API_KEY`
2. `VOICE_TOOLS_OPENAI_KEY`
3. `OPENAI_API_KEY`

Use `OPENAI_REALTIME_API_KEY` if you want a separate key just for Realtime voice.

## 8. Configure environment variables

Edit `~/.hermes/.env`:

```bash
# Discord bot login
DISCORD_BOT_TOKEN=your-discord-bot-token

# Authorization: use your Discord user ID, or trusted role IDs
DISCORD_ALLOWED_USERS=your-discord-user-id
# DISCORD_ALLOWED_ROLES=123456789012345678

# OpenAI Realtime. Prefer a dedicated key when available.
OPENAI_REALTIME_API_KEY=sk-...

# Optional fallback names recognized by Hermes
# VOICE_TOOLS_OPENAI_KEY=sk-...
# OPENAI_API_KEY=sk-...

# Optional: Realtime tuning
OPENAI_REALTIME_MODEL=gpt-realtime
OPENAI_REALTIME_VOICE=alloy
OPENAI_REALTIME_INSTRUCTIONS=You are Hermes on Discord voice. Respond in English only. Keep spoken replies concise. For long research, coding, debugging, browsing, file edits, or multi-step work, call the Hermes subagent handoff tool instead of trying to do everything in voice.

# Optional: Discord realtime turn detection
HERMES_REALTIME_SILENCE_SECONDS=0.65
HERMES_REALTIME_MIN_SPEECH_SECONDS=0.45
HERMES_REALTIME_MIN_RMS=180
```

Notes:

- Keep `.env` out of git.
- Do not paste provider keys into docs, issues, Discord, or commits.
- Empty env vars count as missing; delete empty lines rather than leaving `KEY=` placeholders in production.

## 9. Configure Hermes voice defaults

Edit `~/.hermes/config.yaml` for conservative Hermes-native defaults and optional voice effects:

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
  voice_fx:
    enabled: false
```

The Realtime path uses OpenAI audio output directly; these STT/TTS settings still matter for `/voice join`, voice messages, and fallback testing.

## 10. Run diagnostics before starting the gateway

From a Hermes Agent source checkout:

```bash
python scripts/discord-voice-doctor.py
```

The doctor checks:

- `discord.py` and voice extras
- PyNaCl / AEAD support
- DAVE E2EE package
- local STT and TTS packages
- Opus codec loadability
- `ffmpeg`
- `DISCORD_BOT_TOKEN`
- allowed-user / role posture
- bot login, guild membership, and voice-relevant permissions

If the checkout includes Realtime smoke scripts, run them too:

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

Treat a present key and a usable key as different states. A key can exist but fail because of project, billing, model entitlement, or account-policy problems.

## 11. Start Hermes gateway

Package install:

```bash
hermes gateway
```

Source checkout:

```bash
cd /path/to/hermes-agent
source venv/bin/activate
hermes gateway
```

Keep this process running. For production, run it under launchd, systemd, tmux, or your normal service supervisor.

## 12. Test text before voice

In Discord:

1. DM the bot, or mention it in an allowed server text channel.
2. Ask a simple question.
3. Confirm it responds.
4. Run `/whoami` if slash commands are available, to confirm authorization scope.

Do not debug Realtime voice until normal text and slash command routing work.

## 13. Test Hermes-native voice channel mode

Join a Discord voice channel as the authorized user.

In the linked text channel, run:

```text
/voice join
/voice status
```

Speak a short phrase. Hermes should transcribe and answer using the Hermes-native STT/TTS path.

Leave when done:

```text
/voice leave
```

This narrows Discord RTP/Opus/permissions problems before adding OpenAI Realtime to the path.

## 14. Start OpenAI Realtime voice

Join a Discord voice channel as the authorized user.

In the linked text channel, run:

```text
/voice realtime
```

Hermes should:

1. Verify a Realtime key is configured.
2. Join your current Discord voice channel.
3. Bind the voice session to the text channel where the command was issued.
4. Disable legacy auto-TTS for that channel.
5. Select the OpenAI Realtime voice engine for that guild.

Useful commands:

```text
/voice status
/voice off       # switch live voice engine back to Hermes-native/off behavior
/voice leave     # disconnect and clear voice engine state
```

## 15. Expected Realtime behavior

A healthy Realtime session should:

- answer in spoken English;
- stay concise;
- avoid repeatedly initiating with filler;
- respond when addressed;
- call Hermes subagent handoff for browsing, coding, debugging, research, file edits, and long multi-step tasks;
- post durable subagent results back to the bound text channel;
- inject subagent completion back into the Realtime session so the voice sidecar can summarize or ask whether to read the full result.

## 16. Troubleshooting

### `/voice realtime` says the Realtime key is missing

Set one of these in `~/.hermes/.env` and restart the gateway:

```bash
OPENAI_REALTIME_API_KEY=sk-...
# or VOICE_TOOLS_OPENAI_KEY=sk-...
# or OPENAI_API_KEY=sk-...
```

### Discord slash command does not show `realtime`

This can be a stale Discord application-command registration, not a runtime failure.

Fix path:

1. Restart the gateway and watch command-sync logs.
2. Confirm the local code supports `/voice realtime`.
3. If Discord cached an old schema, patch/resync the application command so `/voice` includes the `realtime` choice.
4. Refresh Discord with Cmd+R / Ctrl+R and reopen the slash command picker.

### Bot joins but hears nothing

Check:

- authorized user is in `DISCORD_ALLOWED_USERS` or has an allowed role;
- the user is not muted/deafened;
- the bot has Connect/Speak/Use Voice Activity;
- Message Content and Server Members intents are enabled;
- `discord.py[voice]`, PyNaCl, davey, Opus, and ffmpeg are installed.

Run:

```bash
python scripts/discord-voice-doctor.py
```

### Realtime replies drift language or get chatty

Set stricter `OPENAI_REALTIME_INSTRUCTIONS`, for example:

```bash
OPENAI_REALTIME_INSTRUCTIONS=You are Hermes on Discord voice. Always respond in English. Speak in one or two concise sentences unless asked for more. Do not initiate repeatedly. For tool work, call start_subagent_task.
```

Restart the gateway after changing instructions.

### Audio cuts off or triggers too often

Tune the turn detector:

```bash
HERMES_REALTIME_SILENCE_SECONDS=0.8
HERMES_REALTIME_MIN_SPEECH_SECONDS=0.5
HERMES_REALTIME_MIN_RMS=220
```

Higher RMS and duration reject more background noise. Higher silence seconds waits longer before committing a turn.

### OpenAI smoke works but Discord voice fails

The OpenAI key is probably fine. Focus on Discord permissions, voice dependencies, Opus, ffmpeg, and voice-channel routing.

### Discord text works but voice fails

Normal Discord text does not prove voice readiness. Voice uses additional permissions and audio dependencies. Run the doctor.

## 17. Security checklist

- [ ] `DISCORD_BOT_TOKEN` stored only in `~/.hermes/.env` or a secret manager.
- [ ] OpenAI key stored only in `~/.hermes/.env` or a secret manager.
- [ ] `.env` is gitignored.
- [ ] `DISCORD_ALLOWED_USERS` or `DISCORD_ALLOWED_ROLES` is set.
- [ ] Bot is invited only to trusted servers.
- [ ] Bot role has only needed permissions.
- [ ] Realtime exposes a narrow safe handoff tool, not raw shell access.
- [ ] Long-running build/research work is delegated to normal Hermes subagents, with durable text-channel output.

## 18. Verification checklist

Run these before calling the setup complete:

```bash
python scripts/discord-voice-doctor.py
python ~/.hermes/skills/devops/hermes-operations/scripts/openai_realtime_text_smoke.py
python ~/.hermes/skills/devops/hermes-operations/scripts/openai_realtime_audio_smoke.py
```

Then verify in Discord:

```text
/whoami
/voice join
/voice status
/voice leave
/voice realtime
/voice status
/voice leave
```

PASS means:

- text bot responds;
- slash commands work;
- voice doctor is green or any warnings are understood/non-blocking;
- OpenAI Realtime smoke returns text/audio;
- `/voice join` works for Hermes-native voice;
- `/voice realtime` joins and responds with OpenAI Realtime audio;
- subagent handoff requests produce a durable text-channel result.

If only the Realtime smoke works, report PARTIAL. If the bot cannot join/hear/speak in Discord, report BLOCKED on Discord voice transport.
