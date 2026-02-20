# Bet365-Odds-Websocket-Delta-Parser
A Python script that connects to Bet365's live odds WebSocket, maintains the session with heartbeats, listens for real-time odds deltas, parses updates into human-readable events, and prints them to the console (with run instructions and implementation notes).

## Getting Set Up

The steps below should help you get set up virtualenv on an Ubuntu system.

```bash
# set up pre-commit so basic linting happens before every commit
pre-commit install
pre-commit run --all-files
```

The steps below should help you get set up the tool on an Ubuntu system.

```shell
# set up virtualenv
python -m venv '.venv'
source .venv/bin/activate

# install requirements
pip install -r requirements.txt
```
