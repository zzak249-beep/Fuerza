# Escáner Granville BingX → Telegram (v2)

Escanea los futuros perpetuos de BingX, detecta señales G1/G3/G4 (long) y G5/G7/G8 (short) al cierre de
cada vela y te avisa por Telegram. **Solo envía señales: no opera y no necesita claves de BingX.**

## Qué hay de nuevo en v2
- **Seguimiento de cada señal** con las mismas reglas de salida que tu estrategia de TradingView
  (TP1 parcial, SL a break-even, trailing, TP2, time-stop, señal contraria). Te avisa de cada hito.
- **Estadísticas reales en R** (win rate, expectancia, profit factor, t-stat) por tipo de señal.
- **Backtester** con la misma lógica: `python bot.py --backtest top:30 --days 60`.
- **Comandos de Telegram**: `/stats`, `/semana`, `/abiertas`, `/estado`. Resumen diario automático.
- Tope de operaciones abiertas, alerta si BingX falla, arranque sin duplicados tras un redeploy.

## Instalación
1. Python 3.10+ → `pip install -r requirements.txt`
2. Copia `_env.example` a `.env` y rellena `TELEGRAM_TOKEN` y `TELEGRAM_CHAT_ID`.
3. `python bot.py --get-chat-id` (tras escribir /start a tu bot) y `python bot.py --test-telegram`.

## Antes de dejarlo corriendo: mide qué señales tienen ventaja
```
python bot.py --backtest top:30 --days 60 --tf 15m          # 30 monedas más líquidas
python bot.py --backtest BTC-USDT,ETH-USDT --days 90 --csv ops.csv
```
Mira "Por tipo de señal": con n ≥ 30 y t < 0 o "sin evidencia", quita ese tipo de `SIGNAL_TYPES`.
Limitaciones: no aplica el filtro de delta (necesita 1m histórico), no modela el tope de posiciones
simultáneas y solo incluye contratos que existen hoy (sesgo de supervivencia).

## Otras comprobaciones
- `python bot.py --list` · `--debug BTC-USDT --tf 15m` · `--history BTC-USDT --tf 15m --bars 200`
- `python bot.py --once --dry-run` → un escaneo completo en consola.

## Railway
1. Sube estos ficheros al repo (incluye `railway.json`). **No subas tu `.env`.**
2. Variables: copia `_env.example` en *Variables → Raw Editor*.
3. Para conservar estadísticas entre redeploys: añade un **Volume** montado en `/data` y define
   `STATE_FILE=/data/state.json` y `TRADES_FILE=/data/trades.json`.
