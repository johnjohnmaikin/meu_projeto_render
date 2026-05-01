from flask import Flask, jsonify
import os
import threading
import time as tm
from datetime import datetime
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin


app = Flask(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TOTALCORNER_URL = "https://www.totalcorner.com/match/today"
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

sinais_enviados = set()

status_bot = {
    "worker_iniciado": False,
    "ultima_execucao": None,
    "ultimo_erro": None,
    "ultimos_sinais_enviados": 0,
    "executando_agora": False
}

execucao_lock = threading.Lock()


ligas = [
    "England Premier League",
    "England Championship",
    "Italy Serie A",
    "Italy Serie B",
    "Spain La Liga",
    "Spain Segunda",
    "Germany Bundesliga I",
    "Germany Bundesliga II",
    "France Ligue 1",
    "France Ligue 2",
    "Austria Bundesliga",
    "Belgium First Division A",
    "Croatia 1.HNL",
    "Czech First League",
    "Holland Eredivisie",
    "Hungary NB I",
    "Poland Ekstraklasa",
    "Portugal Primeira Liga",
    "Romania Liga I",
    "Russia Premier League",
    "Slovenia Prva Liga",
    "Switzerland Super League",
    "Turkey Super Lig",
    "Ukraine Persha Liga",
    "Sweden Allsvenskan",
    "League of Ireland Premier Division",
    "Denmark Superligaen",
    "Japan J-League",
    "South Korea K-League 1",
    "South Africa Premier",
    "Qatar Stars League",
    "Argentina Primera Division",
    "Chile Apertura",
    "USA MLS",
    "Norway Tippeligaen",
    "Mexico Apertura",
    "China Super League",
    "Brazil Serie A",
    "Brazil Serie B",
    "UEFA Champions League",
    "UEFA Europa League",
    "Wales Premier League",
    "Scotland Premiership",
    "AFC Champions League",
    "Australia A-League",
    "Asia - World Cup Qualifying",
    "South America - World Cup Qualifying",
    "Euro 2024 Qualifying",
    "Copa Libertadores",
    "Copa Sudamericana"
]


def normalizar(txt):
    txt = str(txt).strip().lower()
    txt = re.sub(r"\s+", " ", txt)
    txt = re.sub(r"^(?:\d+\s+)+", "", txt)
    txt = re.sub(r"(?:\s+\d+)+$", "", txt)
    return txt.strip()


def limpar_time(txt):
    txt = str(txt).replace("\xa0", " ").strip()
    txt = re.sub(r"^(?:\s*(?:\[\d+\]|\d+)\s*)+", "", txt)
    txt = re.sub(r"(?:\s*\[\d+\]\s*|\s+\d+\s*)+$", "", txt)
    return txt.strip()


def pegar_links_times(df):
    html = requests.get(
        TOTALCORNER_URL,
        headers=HEADERS,
        timeout=20
    ).text

    soup = BeautifulSoup(html, "html.parser")
    base = "https://www.totalcorner.com"

    links_times = {}

    for a in soup.find_all("a", href=True):
        href = a["href"]
        nome = normalizar(a.get_text())

        if "/team/view/" in href and nome:
            links_times[nome] = urljoin(base, href)

    times_unicos = pd.unique(
        df[["Home", "Away"]].astype(str).values.ravel()
    )

    resultado = {}

    for time_nome in times_unicos:
        if str(time_nome).lower() == "nan":
            continue

        time_norm = normalizar(time_nome)
        resultado[time_nome] = links_times.get(time_norm)

    return resultado


def dfs_por_times(links_times):
    dfs_por_time = {}

    for time_nome, link in links_times.items():
        try:
            if not link:
                continue

            data_dos_links = requests.get(
                link,
                headers=HEADERS,
                timeout=20
            )

            tabelas = pd.read_html(data_dos_links.text)[0]

            tabelas.drop(
                columns=[
                    "Time",
                    "Handicap",
                    "Corner",
                    "Corner O/U",
                    "Total Goals",
                    "Goals O/U",
                    "Tips",
                    "Dangerous Attack",
                    "Live Events",
                    "Analysis"
                ],
                inplace=True,
                errors="ignore"
            )

            tabelas.dropna(axis=0, how="any", inplace=True)
            tabelas.rename(columns={"Unnamed: 2": "cronometro"}, inplace=True)

            if "cronometro" not in tabelas.columns:
                continue

            tabelas = tabelas[tabelas["cronometro"] == "Full"]

            if tabelas.empty:
                continue

            tabelas["Score"] = tabelas["Score"].apply(
                lambda a: re.findall(r"\d{1,} - \d{1,}", str(a))
            )

            tabelas = tabelas[tabelas["Score"].apply(lambda x: len(x) > 0)]

            tabelas["Score"] = tabelas["Score"].apply(
                lambda a: list(map(int, a[0].split(" - ")))
            )

            tabelas["Home"] = tabelas["Home"].apply(limpar_time)
            tabelas["Away"] = tabelas["Away"].apply(limpar_time)
            tabelas["Time_Buscado"] = time_nome

            dfs_por_time[time_nome] = tabelas

        except Exception as e:
            print(f"Erro em {time_nome}: {e}", flush=True)

    return dfs_por_time


def enviar_telegram(mensagem):
    bot_token = os.getenv("BOT_TOKEN")
    chat_id = os.getenv("CHAT_ID")

    if not bot_token:
        raise Exception("BOT_TOKEN não configurado no Render.")

    if not chat_id:
        raise Exception("CHAT_ID não configurado no Render.")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": mensagem
    }

    res = requests.post(url, data=payload, timeout=20)

    try:
        resposta = res.json()
    except Exception:
        resposta = {
            "ok": False,
            "erro": res.text
        }

    print("RESPOSTA TELEGRAM:", resposta, flush=True)

    return resposta

def verificar_sinais():
    data = requests.get(
        TOTALCORNER_URL,
        headers=HEADERS,
        timeout=20
    )

    df = pd.read_html(data.text)[0]

    df.dropna(axis=1, how="all", inplace=True)

    df.drop(
        columns=[
            "Handicap",
            "Corner",
            "Total Goals",
            "Dangerous Attack",
            "Analysis"
        ],
        inplace=True,
        errors="ignore"
    )

    df.rename(columns={"Unnamed: 3": "cronometro"}, inplace=True)

    if "cronometro" not in df.columns:
        print("Coluna cronometro não encontrada.", flush=True)
        return 0

    df = df[df["cronometro"].isna()].copy()
    df = df.iloc[2:].reset_index(drop=True)

    if df.empty:
        print("Nenhum jogo encontrado com cronometro nulo.", flush=True)
        return 0

    df["Home"] = df["Home"].apply(limpar_time)
    df["Away"] = df["Away"].apply(limpar_time)

    if "League" in df.columns:
        df = df[df["League"].isin(ligas)]

    if df.empty:
        print("Nenhum jogo encontrado nas ligas filtradas.", flush=True)
        return 0

    links_times = pegar_links_times(df)
    dfs_times = dfs_por_times(links_times)

    if not dfs_times:
        print("Nenhum histórico de times encontrado.", flush=True)
        return 0

    dfs_validos = [x for x in dfs_times.values() if not x.empty]

    if not dfs_validos:
        print("Nenhum dataframe válido encontrado.", flush=True)
        return 0

    df_final = pd.concat(dfs_validos, ignore_index=True)

    resultado = []

    for time_buscado in df_final["Time_Buscado"].dropna().unique():
        jogos_time = df_final[df_final["Time_Buscado"] == time_buscado].copy()

        ultimos_2 = jogos_time.head(2)

        if len(ultimos_2) < 2:
            continue

        gols_feitos = []
        gols_sofridos = []

        for _, row in ultimos_2.iterrows():
            score = row["Score"]

            if not isinstance(score, list) or len(score) < 2:
                gols_feitos.append(None)
                gols_sofridos.append(None)
                continue

            gols_home, gols_away = score[0], score[1]

            if row["Home"] == time_buscado:
                gols_feitos.append(gols_home)
                gols_sofridos.append(gols_away)

            elif row["Away"] == time_buscado:
                gols_feitos.append(gols_away)
                gols_sofridos.append(gols_home)

            else:
                gols_feitos.append(None)
                gols_sofridos.append(None)

        nao_fez_gol = gols_feitos == [0, 0]
        nao_tomou_gol = gols_sofridos == [0, 0]

        if nao_fez_gol or nao_tomou_gol:
            ultimos_2 = ultimos_2.copy()
            ultimos_2["Time_Buscado"] = time_buscado

            if nao_fez_gol and nao_tomou_gol:
                ultimos_2["Tipo_Sinal"] = "Não fez gols e não tomou gols nos últimos 2 jogos"
            elif nao_fez_gol:
                ultimos_2["Tipo_Sinal"] = "Não fez gols nos últimos 2 jogos"
            else:
                ultimos_2["Tipo_Sinal"] = "Não tomou gols nos últimos 2 jogos"

            resultado.append(ultimos_2)

    if resultado:
        df_sinais = pd.concat(resultado, ignore_index=True)
    else:
        print("Nenhum time encontrado nos critérios dos últimos 2 jogos.", flush=True)
        return 0

    sinais_enviados_agora = 0

    for time_buscado in df_sinais["Time_Buscado"].unique():
        jogos_time = df_sinais[
            df_sinais["Time_Buscado"] == time_buscado
        ].reset_index(drop=True)

        if len(jogos_time) < 2:
            continue

        tipo_sinal = jogos_time.iloc[0]["Tipo_Sinal"]

        score1 = jogos_time.iloc[0]["Score"]
        score2 = jogos_time.iloc[1]["Score"]

        jogo1 = f"{jogos_time.iloc[0]['Home']} [{score1[0]} - {score1[1]}] {jogos_time.iloc[0]['Away']}"
        jogo2 = f"{jogos_time.iloc[1]['Home']} [{score2[0]} - {score2[1]}] {jogos_time.iloc[1]['Away']}"

        jogo_atual_df = df[
            (df["Home"] == time_buscado) | (df["Away"] == time_buscado)
        ].reset_index(drop=True)

        if not jogo_atual_df.empty:
            row_atual = jogo_atual_df.iloc[0]

            home_atual = row_atual["Home"]
            away_atual = row_atual["Away"]
            liga_atual = row_atual["League"] if "League" in row_atual.index else ""
            cronometro_atual = row_atual["cronometro"] if "cronometro" in row_atual.index else ""
            score_atual = row_atual["Score"] if "Score" in row_atual.index else ""

            jogo_atual = f"{home_atual} {score_atual} {away_atual}"
        else:
            liga_atual = ""
            cronometro_atual = ""
            jogo_atual = "Jogo atual não encontrado"

        chave_sinal = (time_buscado, tipo_sinal, jogo1, jogo2)

        if chave_sinal in sinais_enviados:
            print(f"Sinal já enviado para {time_buscado}", flush=True)
            continue

        mensagem = f"""🚨 POSSÍVEL SINAL TELEGRAM 🚨

Time: {time_buscado}

Jogo atual:
• {jogo_atual}

Liga: {liga_atual}
Minuto: {cronometro_atual}

Últimos 2 jogos:
• {jogo1}
• {jogo2}

Critério encontrado:
{tipo_sinal}

Monitorar oportunidade no mercado de gols.
"""

        resposta = enviar_telegram(mensagem)
        print(resposta, flush=True)

        if resposta.get("ok"):
            sinais_enviados.add(chave_sinal)
            sinais_enviados_agora += 1

    return sinais_enviados_agora


def worker_loop():
    while True:
        if execucao_lock.acquire(blocking=False):
            try:
                status_bot["executando_agora"] = True
                status_bot["ultimo_erro"] = None

                qtd = verificar_sinais()

                status_bot["ultima_execucao"] = datetime.now().isoformat()
                status_bot["ultimos_sinais_enviados"] = qtd

                print(f"Verificação finalizada. Sinais enviados: {qtd}", flush=True)

            except Exception as e:
                status_bot["ultimo_erro"] = str(e)
                print(f"Erro no loop: {e}", flush=True)

            finally:
                status_bot["executando_agora"] = False
                execucao_lock.release()
        else:
            print("Verificação anterior ainda em andamento.", flush=True)

        print(f"Aguardando {CHECK_INTERVAL_SECONDS} segundos para nova checagem...", flush=True)
        tm.sleep(CHECK_INTERVAL_SECONDS)


def iniciar_worker():
    if status_bot["worker_iniciado"]:
        return

    status_bot["worker_iniciado"] = True

    thread = threading.Thread(
        target=worker_loop,
        daemon=True
    )

    thread.start()


@app.route("/")
def home():
    return jsonify({
        "status": "online",
        "mensagem": "Web Service rodando. Bot ativo em segundo plano.",
        "status_bot": status_bot
    })


@app.route("/status")
def status():
    return jsonify(status_bot)

@app.route("/teste-telegram")
def teste_telegram():
    try:
        resposta = enviar_telegram(
            "✅ Teste: o bot está conectado ao Telegram pelo Render."
        )

        return jsonify({
            "status": "teste_executado",
            "resposta_telegram": resposta
        })

    except Exception as e:
        return jsonify({
            "erro": str(e)
        }), 500

@app.route("/debug-env")
def debug_env():
    return jsonify({
        "BOT_TOKEN_configurado": bool(os.getenv("BOT_TOKEN")),
        "CHAT_ID_configurado": bool(os.getenv("CHAT_ID")),
        "CHAT_ID_valor": os.getenv("CHAT_ID")
    })

@app.route("/telegram-info")
def telegram_info():
    try:
        bot_token = os.getenv("BOT_TOKEN")

        if not bot_token:
            return jsonify({
                "erro": "BOT_TOKEN não configurado"
            }), 500

        url = f"https://api.telegram.org/bot{bot_token}/getMe"

        res = requests.get(url, timeout=20)

        return jsonify(res.json())

    except Exception as e:
        return jsonify({
            "erro": str(e)
        }), 500

@app.route("/rodar-agora")
def rodar_agora():
    if not execucao_lock.acquire(blocking=False):
        return jsonify({
            "erro": "Já existe uma verificação em andamento."
        }), 409

    try:
        status_bot["executando_agora"] = True
        status_bot["ultimo_erro"] = None

        qtd = verificar_sinais()

        status_bot["ultima_execucao"] = datetime.now().isoformat()
        status_bot["ultimos_sinais_enviados"] = qtd

        return jsonify({
            "status": "ok",
            "sinais_enviados": qtd
        })

    except Exception as e:
        status_bot["ultimo_erro"] = str(e)

        return jsonify({
            "erro": str(e)
        }), 500

    finally:
        status_bot["executando_agora"] = False
        execucao_lock.release()


iniciar_worker()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )