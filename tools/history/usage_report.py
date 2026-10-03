"""Print embedding token usage and estimated cost (today / this month / all time) as one JSON line."""
import json

import embed_config
import usage

if __name__ == '__main__':
    cfg = embed_config.load()
    out = usage.summary(cfg['usd_per_million_tokens'])
    out.update(provider=cfg['provider'], model=cfg['model'], usd_per_million_tokens=cfg['usd_per_million_tokens'])
    print(json.dumps(out))
