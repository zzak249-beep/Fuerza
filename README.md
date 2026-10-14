# Escáner Granville BingX → Telegram

Escanea los futuros perpetuos de BingX, detecta señales G1/G3/G4 (long) y G5/G7/G8 (short)
al cierre de cada vela (15m y 5m) y te avisa por Telegram con precio, SL, TP1/TP2 y tamaño sugerido.
**Solo envía señales: no opera y no necesita claves de BingX.**

## Instalación
1. Python 3.10+  →  `pip install -r requirements.txt`
2. Copia `.env.example` a `.env` y rellena `TELEGRAM_TOKEN` (de @BotFather).
3. Escribe `/start` a tu bot y ejecuta `python bot.py --get-chat-id`; pega el número en `TELEGRAM_CHAT_ID`.
4. Prueba: `python bot.py --test-telegram`

## Comprobaciones antes de dejarlo corriendo
- `python bot.py --list` → contratos que se escanearán (comprueba si aparecen las acciones que esperas).
- `python bot.py --debug BTC-USDT --tf 15m` → valores de los filtros en la última vela cerrada.
- `python bot.py --history BTC-USDT --tf 15m --bars 200` → señales de las últimas 200 velas;
  compáralas con los marcadores del indicador de TradingView.
- `python bot.py --once --dry-run` → un escaneo completo imprimiendo en consola.

## Ejecución continua
`python bot.py` (déjalo en un PC o VPS encendido 24/7; p. ej. `nohup python bot.py &` o un servicio systemd).
